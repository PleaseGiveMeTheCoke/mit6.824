package main

import (
	"fmt"
	"time"
)

func main() {
	timer := time.NewTimer(time.Second * 3)

	// 模拟查询数据库
	selectMysql()

	// 如果超时，则抛出异常
	select {
	//超过上面设定的3秒timer.C通道就会传入一个值
	case <-timer.C:
		fmt.Println("超时返回")
		// 超时之后返回错误
		return
	default:
		fmt.Println("没有超时")
	}
	fmt.Println("数据库查询结束,返回")
	return
	// 写自己正常的业务逻辑
}

// 模拟数据查询花了1秒
func selectMysql() {
	time.Sleep(time.Second * 5)
}
