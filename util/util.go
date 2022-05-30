package util

import "fmt"

type RelPathType string

func (p RelPathType) String() string {
	return string(p)
}

func PanicIf(cond bool, msg string) {
	if cond {
		panic(msg)
	}
}

func PanicIfFalse(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

func PanicIfErr(err error, msg string) {
	if err != nil {
		panic(fmt.Sprintf("%v err=%v", msg, err))
	}
}
