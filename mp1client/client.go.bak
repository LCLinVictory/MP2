package mp1client

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type GreReq struct {
	RegPat   string
	Filename string
}

// result
type GrepRes struct {
	MatchRes string
	FileName string
	MatchSuc bool
	MatchCnt int
}

type nodeInfo struct {
	nodeId      int
	nodeService string
}

// generate a list of server nodes
func readServerFromFile(filename string) []nodeInfo {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal("Open File Error!", err)
	}
	defer f.Close()
	buf := bufio.NewReader(f)
	nodeServerList := []nodeInfo{}

	// read line: https://blog.csdn.net/robertkun/article/details/79163905
	lineId := 0
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal("Read File Error!", err)
			}
		}
		lineId += 1
		line = strings.TrimSpace(line)
		node := nodeInfo{lineId, line}
		nodeServerList = append(nodeServerList, node)
	}
	return nodeServerList
}

// call grep function in server by rpc
func connectToServer(node nodeInfo, str string, filename string, c chan GrepRes) {

	client, err := rpc.Dial("tcp", node.nodeService)
	if err != nil {
		mes := "Could not connect to server" + strconv.Itoa(node.nodeId)
		c <- GrepRes{mes, filename, false, 0}
		return
	}

	var reply string
	args := GreReq{str, filename}
	err = client.Call("GrepStr.GrepResult", args, &reply)

	if err != nil {
		mes := "Error during Call"
		c <- GrepRes{mes, filename, false, 0}
		return
	}
	if reply == "" {
		mes := "No match in server" + strconv.Itoa(node.nodeId)
		c <- GrepRes{mes, filename, false, 0}
	} else {
		matchCnt := len(strings.Split(reply, "\n"))
		c <- GrepRes{reply, filename, true, matchCnt}
	}
	client.Close()
}

func RunClientmain(strMessage string, IPList []string) {

	t1 := time.Now()

	// connect to every server, go runtine
	//nodeServerList := readServerFromFile("serverInfo.txt")
	nodeServerList := []nodeInfo{}
	for i, item := range IPList {
		node := nodeInfo{i, item + ":9000"}
		nodeServerList = append(nodeServerList, node)
	}
	c := make(chan GrepRes)

	for _, node := range nodeServerList {
		//filename := "vm" + strconv.Itoa(node.nodeId) + ".log"
		filename := "membership.log"
		go connectToServer(node, strMessage, filename, c)
	}

	/* compute the number of match lines and print
	* print format:
	* filename, total match number
	* line number: match content
	* total match lines count
	 */
	totalCnt := 0
	for i := 0; i < len(nodeServerList); i++ {
		res := <-c
		if res.MatchSuc {
			totalCnt += res.MatchCnt
			fmt.Println("FileName:", res.FileName, "Total Match Number:", res.MatchCnt)
			fmt.Println(res.MatchRes)
		} else {
			fmt.Println(res.MatchRes)
		}
	}
	fmt.Println("Total Match Cnt In All Logs:", totalCnt)

	//compute runtime
	elapsed := time.Since(t1)
	fmt.Println("Total Run Time:", elapsed)
	close(c)
}
