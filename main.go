package main

import(
	"fmt",
	"os",
	"encoding/json"
)

type Address struct{
	City string
	State string
	Country string
	Pincode json.Number
}
type User struct{
	Name string
	Age json.Number
	Company string
	Contact string
	Address Address
}

func main(){
dir:="./" 
}