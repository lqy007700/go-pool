package go_pool

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	stateCreated int32 = 1
	stateRunning int32 = 2
	stateClosing int32 = 3
	stateStopped int32 = 4
	stateLocked  int32 = 5

	errTaskPoolIsNotRunning = errors.New("ekit: TaskPool未运行")
	errTaskPoolIsClosing    = errors.New("ekit：TaskPool关闭中")
	errTaskPoolIsStopped    = errors.New("ekit: TaskPool已停止")
	errTaskPoolIsStarted    = errors.New("ekit：TaskPool已运行")
	errTaskIsInvalid        = errors.New("ekit: Task非法")
	errTaskRunningPanic     = errors.New("ekit: Task运行时异常")

	errInvalidArgument = errors.New("ekit: 参数非法")

	//_            TaskPool = &OnDemandBlockTaskPool{}
	panicBuffLen = 2048

	defaultMaxIdleTime = 10 * time.Second
)

type TaskPool interface {
	// Submit 执行一个任务
	// 如果任务池提供了阻塞的功能，那么如果在 ctx 过期都没有提交成功，那么应该返回错误
	// 调用 Start 之后能否继续提交任务，则取决于具体的实现
	// 调用 Shutdown 或者 ShutdownNow 之后提交任务都会返回错误
	Submit(ctx context.Context, task Task) error

	// Start 开始调度任务执行。在调用 Start 之前，所有的任务都不会被调度执行。
	// Start 之后，能否继续调用 Submit 提交任务，取决于具体的实现
	Start() error

	// Shutdown 关闭任务池。如果此时尚未调用 Start 方法，那么将会立刻返回。
	// 任务池将会停止接收新的任务，但是会继续执行剩下的任务，
	// 在所有任务执行完毕之后，用户可以从返回的 chan 中得到通知
	// 任务池在发出通知之后会关闭 chan struct{}
	Shutdown() (<-chan struct{}, error)

	// ShutdownNow 立刻关闭线程池
	// 任务池能否中断当前正在执行的任务，取决于 TaskPool 的具体实现，以及 Task 的具体实现
	// 该方法会返回所有剩下的任务，剩下的任务是否包含正在执行的任务，也取决于具体的实现
	ShutdownNow() ([]Task, error)
}

type Task interface {
	Run(ctx context.Context) error
}

// TaskFunc 可执行的任务
type TaskFunc func(ctx context.Context) error

func (t TaskFunc) Run(ctx context.Context) error {
	return t(ctx)
}

// BlockQueueTaskPool 并发阻塞任务池
type taskWrapper struct {
	t Task
}

func (tw *taskWrapper) Run(ctx context.Context) (err error) {
	defer func() {
		// 处理 panic
		if r := recover(); r != nil {
			buf := make([]byte, panicBuffLen)
			buf = buf[:runtime.Stack(buf, false)]
			err = fmt.Errorf("%w：%s", errTaskRunningPanic, fmt.Sprintf("[PANIC]:\t%+v\n%s\n", r, buf))
		}
	}()
	return tw.t.Run(ctx)
}

type group struct {
	mp map[int32]int32
	n  int32
	mu sync.RWMutex
}

func (g *group) isIn(id int32) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()

	_, ok := g.mp[id]
	return ok
}

func (g *group) add(id int32) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.mp[id]; !ok {
		g.mp[id] = 1
		g.n++
	}
}

func (g *group) del(id int32) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.mp[id]; ok {
		g.n--
	}
	delete(g.mp, id)
}

func (g *group) size() int32 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.n
}

// OnDemandBlockTaskPool 按需创建goroutine的并发阻塞的任务池
type OnDemandBlockTaskPool struct {
	state             int32
	queue             chan Task
	numGoRunningTasks int32

	totalGo int32
	mutex   sync.RWMutex

	// 初始协程数
	initGo int32
	// 核心协程数
	coreGo int32
	// 最大协程数
	maxGo int32

	// 队列积压率
	queueBacklogRate float64
	shutdownOnce     sync.Once

	// 停止信号
	shutdownDone chan struct{}

	// 协程id方便调试程序
	id int32

	shutdownNowCtx    context.Context
	shutdownNowCancel context.CancelFunc
}

// NewOnDemandBlockTaskPool 创建一个新的 OnDemandBlockTaskPool
// initGo 是初始协程数
// queueSize 是队列大小，即最多有多少个任务在等待调度
// 使用相应的Option选项可以动态扩展协程数
func NewOnDemandBlockTaskPool(initGo int32, queueSize int32) (*OnDemandBlockTaskPool, error) {
	if initGo < 1 {
		return nil, fmt.Errorf("%w：initGo应该大于0", errInvalidArgument)
	}
	if queueSize < 0 {
		return nil, fmt.Errorf("%w：queueSize应该大于等于0", errInvalidArgument)
	}

	b := &OnDemandBlockTaskPool{
		initGo:       initGo,
		coreGo:       initGo,
		maxGo:        initGo,
		queue:        make(chan Task, queueSize),
		shutdownDone: make(chan struct{}, 1),
	}
	atomic.StoreInt32(&b.state, stateCreated)

	b.shutdownNowCtx, b.shutdownNowCancel = context.WithCancel(context.Background())

	return b, nil
}

// Submit 提交一个任务
// 如果此时队列已满，那么将会阻塞调用者。
// 如果因为 ctx 的原因返回，那么将会返回 ctx.Err()
// 在调用 Start 前后都可以调用 Submit
func (o *OnDemandBlockTaskPool) Submit(ctx context.Context, task Task) error {
	if task == nil {
		return fmt.Errorf("%w", errTaskIsInvalid)
	}

	for {
		if atomic.LoadInt32(&o.state) == stateClosing {
			return fmt.Errorf("%w", errTaskPoolIsClosing)
		}

		if atomic.LoadInt32(&o.state) == stateStopped {
			return fmt.Errorf("%w", errTaskPoolIsStopped)
		}

		task = &taskWrapper{t: task}
		ok, err := o.trySubmit(ctx, task, stateCreated)
		// 成功或者错误都return
		if ok || err != nil {
			return err
		}

		ok, err = o.trySubmit(ctx, task, stateRunning)
		if ok || err != nil {
			return err
		}
	}
}

func (o *OnDemandBlockTaskPool) trySubmit(ctx context.Context, task Task, state int32) (bool, error) {
	if atomic.CompareAndSwapInt32(&o.state, state, stateLocked) {
		defer atomic.CompareAndSwapInt32(&o.state, stateLocked, state)

		select {
		case <-ctx.Done():
			return false, fmt.Errorf("%w", ctx.Err())
		case o.queue <- task:
			if state == stateRunning && o.allowToCreateGoroutine() {
				o.increaseTotalGo(1)
				id := atomic.AddInt32(&o.id, 1)
				go o.goroutine(id)
			}
			return true, nil
		default:
			fmt.Println(222)
			return false, nil
		}
	}
	return false, nil
}

func (o *OnDemandBlockTaskPool) increaseTotalGo(n int32) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.totalGo += n
}

func (o *OnDemandBlockTaskPool) allowToCreateGoroutine() bool {
	o.mutex.RLock()
	defer o.mutex.RUnlock()
	if o.totalGo == o.maxGo {
		return false
	}

	// 这个判断可能太苛刻了，经常导致开协程失败，先注释掉
	// allGoShouldBeBusy := atomic.LoadInt32(&b.numGoRunningTasks) == b.totalGo
	// if !allGoShouldBeBusy {
	// 	return false
	// }

	// todo
	rate := float64(len(o.queue)) / float64(cap(o.queue))
	if rate == 0 || rate < o.queueBacklogRate {
		// log.Println("rate == 0", rate == 0, "rate", rate, " < ", b.queueBacklogRate)
		return false
	}

	return true
}

// Start 开始调度任务执行
// Start 之后，调用者可以继续使用 Submit 提交任务
func (o *OnDemandBlockTaskPool) Start() error {
	for {
		if atomic.LoadInt32(&o.state) == stateClosing {
			return fmt.Errorf("%w", errTaskPoolIsClosing)
		}
		if atomic.LoadInt32(&o.state) == stateStopped {
			return fmt.Errorf("%w", errTaskPoolIsStopped)
		}
		if atomic.LoadInt32(&o.state) == stateRunning {
			return fmt.Errorf("%w", errTaskPoolIsStarted)
		}

		if atomic.CompareAndSwapInt32(&o.state, stateCreated, stateLocked) {
			n := o.initGo
			allowGo := o.maxGo - o.initGo
			needGo := int32(len(o.queue)) - o.initGo
			if needGo > 0 {
				if needGo <= allowGo {
					n += needGo
				} else {
					n += allowGo
				}
			}
			o.increaseTotalGo(n)
			for i := int32(0); i < n; i++ {
				go o.goroutine(atomic.AddInt32(&o.id, 1))
			}

			atomic.CompareAndSwapInt32(&o.state, stateLocked, stateRunning)
			return nil
		}
	}
}

// Shutdown 将会拒绝提交新的任务，但是会继续执行已提交任务
// 当执行完毕后，会往返回的 chan 中丢入信号
// Shutdown 会负责关闭返回的 chan
// Shutdown 无法中断正在执行的任务
func (o *OnDemandBlockTaskPool) Shutdown() (<-chan struct{}, error) {
	for {

		if atomic.LoadInt32(&o.state) == stateCreated {
			return nil, fmt.Errorf("%w", errTaskPoolIsNotRunning)
		}

		if atomic.LoadInt32(&o.state) == stateStopped {
			return nil, fmt.Errorf("%w", errTaskPoolIsStopped)
		}

		if atomic.LoadInt32(&o.state) == stateClosing {
			return nil, fmt.Errorf("%w", errTaskPoolIsClosing)
		}

		if atomic.CompareAndSwapInt32(&o.state, stateRunning, stateClosing) {
			// 目标：不但希望正在运行中的任务自然退出，还希望队列中等待的任务也能启动执行并自然退出
			// 策略：先将队列中的任务启动并执行（清空队列），再等待全部运行中的任务自然退出

			// 先关闭等待队列不再允许提交
			// 同时工作协程能够通过判断b.queue是否被关闭来终止获取任务循环
			close(o.queue)
			return o.shutdownDone, nil
		}
	}
}

func (o *OnDemandBlockTaskPool) ShutdownNow() ([]Task, error) {
	for {

		if atomic.LoadInt32(&o.state) == stateCreated {
			return nil, fmt.Errorf("%w", errTaskPoolIsNotRunning)
		}

		if atomic.LoadInt32(&o.state) == stateClosing {
			return nil, fmt.Errorf("%w", errTaskPoolIsClosing)
		}

		if atomic.LoadInt32(&o.state) == stateStopped {
			return nil, fmt.Errorf("%w", errTaskPoolIsStopped)
		}

		if atomic.CompareAndSwapInt32(&o.state, stateRunning, stateStopped) {
			// 目标：立刻关闭并且返回所有剩下未执行的任务
			// 策略：关闭等待队列不再接受新任务，中断工作协程的获取任务循环，清空等待队列并保存返回

			close(o.queue)

			// 发送中断信号，中断工作协程获取任务循环
			o.shutdownNowCancel()

			// 清空队列并保存
			tasks := make([]Task, 0, len(o.queue))
			for task := range o.queue {
				tasks = append(tasks, task)
			}
			return tasks, nil
		}
	}
}

func (o *OnDemandBlockTaskPool) goroutine(id int32) {
	for {
		select {
		case <-o.shutdownNowCtx.Done():
			o.decreaseTotalGo(1)
			return
		case task, ok := <-o.queue:
			atomic.AddInt32(&o.numGoRunningTasks, 1)
			if !ok {
				// b.numGoRunningTasks > 1表示虽然当前协程监听到了b.queue关闭但还有其他协程运行task，当前协程自己退出就好
				// b.numGoRunningTasks == 1表示只有当前协程"运行task"中，其他协程在一定在"拿到b.queue到已关闭"，这一信号的路上
				// 绝不会处于运行task中
				if atomic.CompareAndSwapInt32(&o.numGoRunningTasks, 1, 0) && atomic.LoadInt32(&o.state) == stateClosing {
					o.shutdownOnce.Do(func() {
						// 状态迁移
						atomic.CompareAndSwapInt32(&o.state, stateClosing, stateStopped)
						// 显示通知外部调用者
						o.shutdownDone <- struct{}{}
						close(o.shutdownDone)
					})

					o.decreaseTotalGo(1)
					return
				}
				// 有其他协程运行task中，自己退出就好。
				atomic.AddInt32(&o.numGoRunningTasks, -1)
				o.decreaseTotalGo(1)
				return
			}

			_ = task.Run(o.shutdownNowCtx)
			atomic.AddInt32(&o.numGoRunningTasks, -1)

			o.mutex.Lock()
			// log.Println("id", id, "totalGo-mem", b.totalGo-b.timeoutGroup.size(), "totalGo", b.totalGo, "mem", b.timeoutGroup.size())
			if o.coreGo < o.totalGo && (len(o.queue) == 0 || int32(len(o.queue)) < o.totalGo) {
				// 协程在(coreGo,maxGo]区间
				// 如果没有任务可以执行，或者被判定为可能抢不到任务的协程直接退出
				// 注意：一定要在此处减1才能让此刻等待在mutex上的其他协程被正确地分区
				o.totalGo--
				// log.Println("id", id, "exits....")
				o.mutex.Unlock()
				return
			}

			//if o.initGo < o.totalGo-o.timeoutGroup.size() /* && len(b.queue) == 0 */ {
			//	// log.Println("id", id, "initGo", b.initGo, "totalGo-mem", b.totalGo-b.timeoutGroup.size(), "totalGo", b.totalGo)
			//	// 协程在(initGo，coreGo]区间，如果没有任务可以执行，重置计时器
			//	// 当len(b.queue) != 0时，即便协程属于(coreGo,maxGo]区间，也应该给它一个定时器兜底。
			//	// 因为现在看队列中有任务，等真去拿的时候可能恰好没任务，如果不给它一个定时器兜底此时就会出现当前协程总数长时间大于始协程数（initGo）的情况。
			//	// 直到队列再次有任务时才可能将当前总协程数准确无误地降至初始协程数，因此注释掉len(b.queue) == 0判断条件
			//	idleTimer = time.NewTimer(o.maxIdleTime)
			//	o.timeoutGroup.add(id)
			//	// log.Println("id", id, "add timeoutGroup", "size", b.timeoutGroup.size())
			//}
			o.mutex.Unlock()
		}
	}
}

func (o *OnDemandBlockTaskPool) decreaseTotalGo(n int32) {
	o.mutex.Lock()
	o.totalGo -= n
	o.mutex.Unlock()
}
