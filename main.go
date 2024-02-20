package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ЗАДАНИЕ:
// * сделать из плохого кода хороший;
// * важно сохранить логику появления ошибочных тасков;
// * сделать правильную мультипоточность обработки заданий.
// Обновленный код отправить через merge-request.

// приложение эмулирует получение и обработку тасков, пытается и получать и обрабатывать в многопоточном режиме
// В конце должно выводить успешные таски и ошибки выполнены остальных тасков

// Task содержит минимально необходимую информацию для запуска и обработки таски
type Task struct {
	id         uuid.UUID // заменила тип на uuid, т.к. int не подходит для хранения идентификаторов
	createTime time.Time // время создания
	endTime    time.Time // время выполнения
	err        error     // ошибка выполнения таски
}

// Processor - обработчик таск, умеет запускать их и обрабатывать результаты
type Processor struct {
	doneTasks   []uuid.UUID    // успешные таски
	undoneTasks []error        // таски с ошибками
	workerWG    sync.WaitGroup // синхронизация воркеров (необходимо для graceful stop)
	sorterWG    sync.WaitGroup // синхронизация сортировщика (необходимо для graceful stop)
	sema        chan struct{}  // семафор, для того чтобы избежать запуска миллиона горутин единовременно
}

func main() {
	// использую контекст для graceful stop
	// с учетом условий по времени также подошел бы context.WithTimeout,
	// но выбрала этот, т.к. ставила задачу имитировать реальное приложение, а не тест
	ctx, cancel := context.WithCancel(context.Background())

	tasks := creator(ctx)
	p := NewProcessor(tasks, 100)

	time.Sleep(time.Second * 3)
	cancel()

	done, undone := p.Results()

	println("Errors:")
	for _, err := range undone {
		println(err.Error())
	}

	println("Done tasks:")
	for _, id := range done {
		println(id.String())
	}
}

// creator создает новые таски пока жив контекст, иногда добавляя в них ошибку случайным образом
func creator(ctx context.Context) <-chan *Task {
	tasks := make(chan *Task)
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(tasks)
				return
			default:
				var err error
				if time.Now().Nanosecond()%2 > 0 { // вот такое условие появления ошибочных тасков
					err = fmt.Errorf("something wrong")
				}
				tasks <- &Task{
					createTime: time.Now(),
					id:         uuid.New(), // генерирую так, старый способ генерировал одинаковые идентификаторы
					err:        err,
				} // передаем таск на выполнение
			}
		}
	}()
	return tasks
}

// NewProcessor создает обработчик тасок: запускает их, сортирует и может вернуть результат.
// Принимает канал с новыми тасками и количество потоков, в которое можно их обрабатывать
func NewProcessor(tasks <-chan *Task, limit int) *Processor {
	p := &Processor{
		sema: make(chan struct{}, limit),
	}

	sort := p.process(tasks)
	go p.sort(sort)

	return p
}

// process получает таски из канала и "запускает" их
func (p *Processor) process(tasks <-chan *Task) <-chan *Task {
	sort := make(chan *Task)
	go func() {
		for task := range tasks {
			p.sema <- struct{}{}
			p.workerWG.Add(1)
			go p.work(task, sort)
		}
		p.workerWG.Wait()
		close(sort)
		close(p.sema)
	}()
	return sort
}

// work имитирует запуск и работу конкретной таски, и отправляет результат в сортировщик
func (p *Processor) work(task *Task, sort chan *Task) {
	// так как в первоначальном коде при ошибке в дату писался текст и парсинг прошел бы неверно,
	// предполагаю, что можно исключить это условие и опереться только на пришедшую ошибку
	time.Sleep(time.Millisecond * 150)
	task.endTime = time.Now()
	sort <- task
	p.workerWG.Done()
	<-p.sema
}

// sort сортирует результаты таск и распределяет их по слайсам удачных и неудачных
func (p *Processor) sort(sort <-chan *Task) {
	p.sorterWG.Add(1)
	for task := range sort {
		if task.err == nil {
			p.doneTasks = append(p.doneTasks, task.id)
		} else {
			p.undoneTasks = append(p.undoneTasks, fmt.Errorf(
				"task id %s time %v, error: %w",
				task.id,
				task.createTime.Format("2006-01-02 15:04:05"),
				task.err,
			))
		}
	}
	p.sorterWG.Done()
}

func (p *Processor) Results() ([]uuid.UUID, []error) {
	p.sorterWG.Wait()
	return p.doneTasks, p.undoneTasks
}
