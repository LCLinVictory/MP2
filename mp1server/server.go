package mp1server

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
)

type GrepStr string

type GreReq struct {
	RegPat   string
	Filename string
}

//run grep command on server and get the results: https://blog.csdn.net/qq_36874881/article/details/78234005
func (s *GrepStr) GrepResult(req GreReq, reply *string) error {

	commandName := "grep"
	params := []string{"-n"}
	path := "../log/" + req.Filename
	//fmt.Println("Path = ", path)
	params = append(params, req.RegPat, path)
	//fmt.Println("grep -> ", params)
	cmd := exec.Command(commandName, params...)

	/* output the grep results */
	output, err := cmd.Output()
	if err != nil {
		fmt.Println("Grep Error: ", err)
	}
	//fmt.Println("cmd output: ", string(output))
	*reply = string(output)
	*reply = strings.TrimSpace(*reply) //delete the blank line
	//fmt.Println("grep result : ", reply)
	return nil
}

func checkError(err error) {
	if err != nil {
		log.Fatal("Error: ", err.Error())
		os.Exit(1)
	}
}

// rpc based on TCP protocol, wait for the client to call: https://blog.csdn.net/qq_34777600/article/details/81159443
func RunServermain() {
	strMessage := new(GrepStr)
	rpc.Register(strMessage)

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":9010")
	checkError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	fmt.Println("Start Listening!")
	checkError(err)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}
