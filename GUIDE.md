package main

import (
"fmt"
"sync"
)

var waitgroup = sync.WaitGroup{}
func main(){
var intNum int = 10;
fmt.Println(intNum)

    waitgroup.Add(1)
    go printMe("Hello World")
    // run concurrent using go (goroutines)
    // can use var m = sync.Mutex{} / sync.RWMutex{} for multiaccess data
    // locking data on edit like actual database, prevent race conditions

    //m.Lock() / m.RLock()
    // execution block
    // m.Unlock() / m.RUnlock
    waitgroup.Wait()

    var newuser User

    newuser.name = "John"
    var output string = newuser.BuildUser("johndoe@gmail.com")

    fmt.Println(output)

    var anotheruser User
    fmt.Println(anotheruser, newuser)

    newuser.userID = "123456"

    fmt.Println(newuser)

}

func printMe(value string){
fmt.Println(value)
fmt.Println(&value)
waitgroup.Done()
}

type User struct {
userID string
name string
}

func (usr User) BuildUser(email string) string {
return "Hello, World! I am " + usr.name + " with email: " + email
}
