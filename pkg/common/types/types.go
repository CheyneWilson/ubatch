package types

type Id int

type Result[T any] struct {
	Id  Id
	Ok  T
	Err error
}

type Job[T any] struct {
	Id   Id
	Data T
}

type BatchProcessor[T any, R any] interface {
	Process(jobs []Job[T]) []Result[R]
}
