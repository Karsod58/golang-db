package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jcelliott/lumber"
)
const Version="1.0.1"
type (
	Logger interface{
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}
	Driver struct{
		mutex sync.Mutex
		mutexes map[string]*sync.Mutex
		dir string
		log Logger
	}
)
type Options struct{
	Logger 
}
func stat(path string)(fi os.FileInfo,err error){
	if fi,err=os.Stat(path); os.IsNotExist(err){
		fi,err=os.Stat(path+".json")
	}
	return 
}
func New(dir string,options *Options)(*Driver, error){
dir=filepath.Clean(dir)
opts:=Options{}
if options!=nil{
	opts=*options
}
if opts.Logger==nil{
	opts.Logger=lumber.NewConsoleLogger((lumber.INFO))
}
driver:=Driver{
	dir: dir,
	mutexes: make(map[string]*sync.Mutex),
	log: opts.Logger,
}
 if _,err:=os.Stat(dir); err==nil{
	opts.Logger.Debug("Using '%s' (database already exists) \n ",dir)
	return &driver,nil
 }
opts.Logger.Debug("Creating the database at '%s' ... \n",dir)
return &driver,os.Mkdir(dir,0755)
}
func(d *Driver) Write(collection, resource string, v interface{}) error {
if collection==""{
	return fmt.Errorf("Missing collection -  no place to save record")
}
if resource==""{
	return fmt.Errorf("Missing resource- unable to save record (no name)!")
}
mutex:=d.getOrCreateMutex(collection)
mutex.Lock()
defer mutex.Unlock()
dir:=filepath.Join(d.dir,collection)
finalPath:=filepath.Join(dir,resource+".json")
tempPath:= finalPath+".tmp"
if err:=os.MkdirAll(dir,0755) ; err!=nil{
	return  err
}

b,err:=json.MarshalIndent(v,"","\t")
if err!=nil{
	return err
}
b=append(b,byte('\n'))
e:=os.WriteFile(tempPath,b,0644)
if e!=nil{
	return err
}
return os.Rename(tempPath,finalPath)
}
func(d *Driver) Read(collection,resource string,v interface{}) error {
if collection==""{
	return  fmt.Errorf("Missing collection- no place to read record!")
}
if resource==""{
	return  fmt.Errorf("Missing resource -unable to read record (no name)!")
}
record:=filepath.Join(d.dir,collection,resource)
 _,err:=stat(record)
if err!=nil{
	return err
}
b,err:=os.ReadFile(record+".json")
if err!=nil{
	return err
}
return json.Unmarshal(b,&v)
}
func(d *Driver) ReadAll(collection string)([]string,error){
if collection==""{
	return nil,fmt.Errorf("Missing collection  - unable to read record")
}
dir:=filepath.Join(d.dir,collection)
if _,err:=stat(dir);err!=nil{
	return nil,err
}
files,_:=os.ReadDir(dir)
 var records []string

 for _,file:=range files{
	b,err:=os.ReadFile(filepath.Join(dir,file.Name()))
	if err!=nil{
		return  nil,err
	}
	records=append(records, string(b))
 }
return records,nil
}
func(d *Driver) Delete(collection,resource string) error{
path:=filepath.Join(collection,resource)
mutex:=d.getOrCreateMutex(collection)
mutex.Lock()
defer mutex.Unlock()
dir:=filepath.Join(d.dir,path)
switch fi,err:=stat(dir);{
case fi==nil,err!=nil:
	return  fmt.Errorf("unable to find o directory named %v \n",path)
case fi.Mode().IsDir():
	return os.RemoveAll(dir)
case fi.Mode().IsRegular():
	return os.RemoveAll(dir+".json")	
}
return  nil
}
func(d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
m,ok:=d.mutexes[collection]
if !ok {
	m=&sync.Mutex{}
	d.mutexes[collection]=m

}
return  m
}
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
func handleCommand(db *Driver, input string) {
	args := strings.Fields(input)
	if len(args) == 0 {
		return
	}

	switch args[0] {

	case "create":
		if len(args) < 10 || args[1] != "user" {
			fmt.Println("Usage: create user <name> <age> <company> <contact> <city> <state> <country> <pincode>")
			return
		}

		user := User{
			Name:    args[2],
			Age:     json.Number(args[3]),
			Company: args[4],
			Contact: args[5],
			Address: Address{
				City:    args[6],
				State:   args[7],
				Country: args[8],
				Pincode: json.Number(args[9]),
			},
		}

		if err := db.Write("users", user.Name, user); err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println("User created successfully")

	case "get":
		if len(args) < 2 {
			fmt.Println("Usage: get user <name> | get users")
			return
		}

		if args[1] == "users" {
			records, err := db.ReadAll("users")
			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			for _, r := range records {
				fmt.Println(r)
			}
			return
		}

		if args[1] == "user" && len(args) == 3 {
			user := User{}
			if err := db.Read("users", args[2], &user); err != nil {
				fmt.Println("Error:", err)
				return
			}
			b, _ := json.MarshalIndent(user, "", "  ")
			fmt.Println(string(b))
			return
		}

	case "update":
		if len(args) != 5 || args[1] != "user" {
			fmt.Println("Usage: update user <name> <field> <value>")
			return
		}

		user := User{}
		if err := db.Read("users", args[2], &user); err != nil {
			fmt.Println("User not found")
			return
		}

		switch strings.ToLower(args[3]) {
		case "company":
			user.Company = args[4]
		case "contact":
			user.Contact = args[4]
		case "age":
			user.Age = json.Number(args[4])
		default:
			fmt.Println("Unsupported field")
			return
		}

		db.Write("users", user.Name, user)
		fmt.Println("User updated")

	case "delete":
		if len(args) != 3 || args[1] != "user" {
			fmt.Println("Usage: delete user <name>")
			return
		}

		if err := db.Delete("users", args[2]); err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println("User deleted")

	default:
		fmt.Println("Unknown command")
	}
}


func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Simple Go NoSQL DB (type 'exit' to quit)")

	fmt.Print("Enter DB directory: ")
	dir, _ := reader.ReadString('\n')
	dir = strings.TrimSpace(dir)

	db, err := New(dir, nil)
	if err != nil {
		fmt.Println("Failed to create DB:", err)
		return
	}

	for {
		fmt.Print("> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		if line == "exit" {
			break
		}

		handleCommand(db, line)
	}
}
