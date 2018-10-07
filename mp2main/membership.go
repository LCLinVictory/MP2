package main

import (
	"log"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
	"sync"
	"strings"
	"math/rand"
	"../mp1server"
	"../mp1client"
)

type MesInfoEntry struct {
	IpAddr      string
	Timestamp   string
	Type        string
	PgyBackList []MemEntry
}

type MemEntry struct {
	Id     string
	IpAddr string
}

const (
	MIN_LIST_SIZE		= 4
	PING_PERIOD			= time.Second * 1 
	ACK_TIMEOUT			= time.Millisecond * 2000
	PACKET_LOSS_RATE	= 0.0	// 0.0 ~ 1.0
)

var (
	JoinIp			string = "172.22.158.138"
	JoinPort		string = "9001"
	MessagePort		string = "9002"
	MembershipList 		   = make([]MemEntry, 0)
	PiggybackedList		   = make([]MemEntry, 0)
	LocalIp 		string = getLocalIp()
	ACKtimers		[3]*time.Timer 
	resetTimerFlags	[3]int
	mutex           = &sync.Mutex{}
	InfoLog *log.Logger
	ErrorLog *log.Logger
)

// https://blog.csdn.net/yxys01/article/details/78054757
// get the local ip address
func getLocalIp() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		ErrorLog.Println("can not get the local ipAddr:", err)
		os.Exit(1)
	}

	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String()
			}
		}
	}
	return ""
}

func sendMessage(mesInfo MesInfoEntry, receiverList []string, port string) {
	// https://colobu.com/2014/12/02/go-socket-programming-UDP/

	var buf bytes.Buffer
	for _, receiverIP := range receiverList {
		addr, err := net.ResolveUDPAddr("udp", receiverIP+":"+port)
		if err != nil {
			ErrorLog.Println("Can not resolve the address:", err)
			os.Exit(1)
		}

		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			ErrorLog.Println("Can not dial during sendMessage:", err)
			os.Exit(1)
		}
		defer conn.Close()
		err = json.NewEncoder(&buf).Encode(mesInfo)
		if err != nil {
			ErrorLog.Println("can not encode into json:", err)
			os.Exit(1)
		}

		/* Packet loss simulate */
		// https://blog.csdn.net/wo198711203217/article/details/72900742
		rand.Seed(time.Now().UnixNano())
		tmpflag := rand.Intn(100)
		if tmpflag >= PACKET_LOSS_RATE * 100 {
			_, err = conn.Write(buf.Bytes())
			//fmt.Println("In sendMessage: Write message =", string(buf.Bytes()))
		} else {
			fmt.Println("Packet loss simulate --- PACKET_LOSS_RATE : ", PACKET_LOSS_RATE)
		}

	}
}

// a new node send message to introducer to add into the membership list
func addToMemship() {

	var receiverList = make([]string, 1)
	piggyList := make([]MemEntry, 0)
	JoinMessage := MesInfoEntry{
		IpAddr:      LocalIp,
		Timestamp:   "",
		Type:        "Join",
		PgyBackList: piggyList,
	}
	receiverList[0] = JoinIp
	port := JoinPort
	sendMessage(JoinMessage, receiverList, port)
}

// all nodes monitor JoinPort of the introducer and add new node into membership list or get a membership list
func listenToIntro() {
	addr, err := net.ResolveUDPAddr("udp", ":"+JoinPort)
	if err != nil {
		fmt.Println("Can not resolve addr during listenToIntro:", err)
		os.Exit(1)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		ErrorLog.Println("Can not listenUDP during listenToIntro:", err)
		os.Exit(1)
	}
	defer conn.Close()
	
	for {
		entryList := make([]MemEntry, 0)
		buf := make([]byte, 1024)
		n, _, err := conn.ReadFromUDP(buf)
		err = json.NewDecoder(bytes.NewReader(buf[:n])).Decode(&entryList)
		
		if err != nil {
			ErrorLog.Println("Can not decode during listenToIntro:", err)
			os.Exit(1)
		}
		mutex.Lock()
		if len(entryList) == 1 {
			MembershipList = append(MembershipList, entryList[0])
			InfoLog.Println("Member ID:", entryList[0].Id, " joins into my group")
		} else {
			MembershipList = entryList
			InfoLog.Println("Join into the group with Member ID:", MembershipList[getIx(LocalIp)].Id)
		}
		mutex.Unlock()
		//fmt.Println("MembershipList in listenToIntro Now:", MembershipList)
	}
}

// introducer send membership list to the new node and info about new node to other active nodes
func broadCast(entry MemEntry) {
	var bufOther bytes.Buffer
	var bufNew bytes.Buffer
	MemberNew := make([]MemEntry, 1)
	MemberNew[0] = entry

	for _, member := range MembershipList {
		if member.IpAddr != LocalIp {
			addr, err := net.ResolveUDPAddr("udp", member.IpAddr+":"+JoinPort)
			if err != nil {
				ErrorLog.Println("Can not resolve addr during broadCast:", err)
				os.Exit(1)
			}
			conn, err := net.DialUDP("udp", nil, addr)
			if err != nil {
				ErrorLog.Println("Can not dialUDP during broadCast:", err)
				os.Exit(1)
			}
			defer conn.Close()
			if member.IpAddr != entry.IpAddr {
				err = json.NewEncoder(&bufOther).Encode(MemberNew)
				if err != nil {
					ErrorLog.Println("Can not encodeToOther during broadcast:", err)
					os.Exit(1)
				}
				conn.Write(bufOther.Bytes())
				//fmt.Println("In broadcast!", string(bufOther.Bytes()))
			} else {
				err = json.NewEncoder(&bufNew).Encode(MembershipList)
				if err != nil {
					ErrorLog.Println("Can not encode during broadCastToNew:", err)
					os.Exit(1)
				}
				conn.Write(bufNew.Bytes())
				//fmt.Println("In broadcast!", string(bufNew.Bytes()))
			}
		}
	}
}

/*
 * https://www.jb51.net/article/64705.htm
 */
func checkTs(Mentry MesInfoEntry) bool {
	checkVal := true
	for _, member := range MembershipList {
		if member.IpAddr == Mentry.IpAddr {
			t1 := strings.Split(member.IpAddr, "+")[1]
			t2 := Mentry.Timestamp
			time1, err := time.Parse("2006-01-02 15:04:05", t1)
			time2, err := time.Parse("2006-01-02 15:04:05", t2)
			if err == nil && time1.After(time2) {
				checkVal = false
			}
		}
	}
	return checkVal
}

/*
 * Get index of current host
 */
func getIx(targetIP string) int {
//func getIx() int {
	for i, element := range MembershipList {
		if targetIP == element.IpAddr {
			return i
		}
	}
	return -1
}

/*
 * Function to give the relative location of the host with respect to the current node in the ML
 */
func getRelativeIx(targetIP string) int {
	localIx := getIx(LocalIp)
	targetIx := getIx(targetIP)
	MemshipNum := len(MembershipList)
	relativeIx := (targetIx - localIx) % MemshipNum
	if relativeIx == 1 || relativeIx == 2 || relativeIx == 3 {
		return relativeIx
	}
	return -1
}

func resetCorrespondingTimers() {
	for i := 0; i < 3; i++ {
		resetTimerFlags[i] = 1
		ACKtimers[i].Reset(0)
	}
}

/*
 * This function sends Ping messages to next three successive neighbours every PING_PERIOD
 */
func sendPing() {
	for {
		MemshipNum := len(MembershipList)
		if MemshipNum >= MIN_LIST_SIZE {
			var receiverList = make([]string, 3)
			formatTimeStr := time.Unix(time.Now().Unix(), 0).Format("2006-01-02 15:04:05")
			PingMessage := MesInfoEntry{
				IpAddr:  		LocalIp,
				Timestamp:		formatTimeStr,
				Type:			"PING",
				PgyBackList:	PiggybackedList,
			}
			for i := 0; i < 3; i++ {
				receiverList[i] = MembershipList[(getIx(LocalIp)+i+1)%MemshipNum].IpAddr
			}
			sendMessage(PingMessage, receiverList, MessagePort)
		}
		time.Sleep(PING_PERIOD)
	}
}

/*
 * This function would ...
 * (i+1)%N, (i+2)%N, (i+3)%N. N=Total number of nodesin the memeber. This method is called for relativeindex 1,2 and 3
 */
func checkAck(relativeIx int) {

	for len(MembershipList) < MIN_LIST_SIZE {
		time.Sleep(100 * time.Millisecond)
	}

	relativeIP := MembershipList[(getIx(LocalIp)+relativeIx)%len(MembershipList)].IpAddr

	ACKtimers[relativeIx-1].Reset(ACK_TIMEOUT)	// !!!
	<-ACKtimers[relativeIx-1].C 				// waiting to be triggered

	mutex.Lock()
	if len(MembershipList) >= MIN_LIST_SIZE && getRelativeIx(relativeIP) == relativeIx && resetTimerFlags[relativeIx-1] != 1 {
		/* Failure detected first time */
		//fmt.Println("Failure detected at IpAddr : ", relativeIP)
		ErrorLog.Println("Fail to detect at IpAddr:", relativeIP)
		targetIx := getIx(relativeIP)
		node := MemEntry{MembershipList[targetIx].Id, relativeIP}
		PiggybackedList = append(PiggybackedList, node)
		InfoLog.Println("Detect! Node ID:", node.Id, "fails!")
		/* Update MembershipList */
		MembershipList = append(MembershipList[:targetIx], MembershipList[targetIx+1:]...)
		
	}
	// None of of the Events should be updating the MembershipList , only then this condition would be set.
	// Reset all the other timers (which the current node is monitoring) as well if the above condition is met
	if resetTimerFlags[relativeIx-1] == 0 {
		InfoLog.Println("Force stopping other timers :", string(relativeIx))
		for i := 1; i < 3; i++ {
			resetTimerFlags[i] = 1
			ACKtimers[i].Reset(0)	// !!!
		}
	} else {
		resetTimerFlags[relativeIx-1] = 0
	}

	mutex.Unlock()
	go checkAck(relativeIx)

}

// introducer waits for new node
func introAddNode() {

	addr, err := net.ResolveUDPAddr("udp", JoinIp+":"+JoinPort)
	if err != nil {
		fmt.Println("Can not resolve the address:", err)
		os.Exit(1)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		ErrorLog.Println("Can not listen:", err)
		os.Exit(1)
	}
	defer conn.Close()

	for {
		Mentry := MesInfoEntry{}
		buf := make([]byte, 1024)
		n, _, err := conn.ReadFromUDP(buf)
		err = json.NewDecoder(bytes.NewReader(buf[:n])).Decode(&Mentry)
		if err != nil {
			ErrorLog.Println("Json decode failed:", err)
			os.Exit(1)
		}
		//fmt.Println("IntroAddNode:", Mentry)
		formatTimeStr := time.Unix(time.Now().Unix(), 0).Format("2006-01-02 15:04:05")
		ip := Mentry.IpAddr
		entry := MemEntry{
			Id:     ip + "+" + formatTimeStr,
			IpAddr: ip,
		}
		/*check timestamp pass*/
		if checkTs(Mentry) {
			mutex.Lock()
			resetCorrespondingTimers()
			MembershipList = append(MembershipList, entry)
			InfoLog.Println("Member ID:", entry.Id, " joins into my group")
			mutex.Unlock()
			broadCast(entry)
		}
	}
}

/*
 * Listen to messages on UDP port from other nodes and take appropriate action. Possible message types are
 * PING, ACK, ...
 */
func listenMessages() {
	addr, err := net.ResolveUDPAddr("udp", ":"+MessagePort)
	if err != nil {
		ErrorLog.Println("Can not resolve addr during listenMessages:", err)
		os.Exit(1)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		ErrorLog.Println("Can not listenUDP during listenMessages:", err)
		os.Exit(1)
	}
	defer conn.Close()

	for {
		msg := MesInfoEntry{}
		buf := make([]byte, 1024)
		n, _, err := conn.ReadFromUDP(buf)
		err = json.NewDecoder(bytes.NewReader(buf[:n])).Decode(&msg)
		//fmt.Println("IN listenMessages:", msg)
		if err != nil {
			ErrorLog.Println("Can not decode during listenMessages:", err)
			os.Exit(1)
		}
		switch msg.Type {
		case "PING":
			var receiverList = make([]string, 1)
			formatTimeStr := time.Unix(time.Now().Unix(), 0).Format("2006-01-02 15:04:05")
			piggyList := make([]MemEntry, 0)
			ACKMessage := MesInfoEntry{
				IpAddr:  		LocalIp,
				Timestamp:		formatTimeStr,
				Type:			"ACK",
				PgyBackList:	piggyList,
			}
			receiverList[0] = msg.IpAddr
			sendMessage(ACKMessage, receiverList, MessagePort)

			/* Find target IpAddr in MembershipList */
			mutex.Lock()
			if len(msg.PgyBackList) > 0 {
				for _, member := range msg.PgyBackList {
					targetIx := getIx(member.IpAddr)
					if targetIx != -1 && member.Id == MembershipList[targetIx].Id  {
						/* Update MembershipList */
						resetCorrespondingTimers()
						MembershipList = append(MembershipList[:targetIx], MembershipList[targetIx+1:]...)
						/* Update PiggybackedList : append */
						PiggybackedList = append(PiggybackedList, member)
						InfoLog.Println("Receive Fail Info! Node ID: ", member.Id, "fails")
					} else {
						var pgbIx = -1
						for i, element := range PiggybackedList {
							if member.Id == element.Id {
								pgbIx = i
								break
							}
						}
						if pgbIx != -1 {
							/* Update PiggybackedList : delete */
							PiggybackedList = append(PiggybackedList[:pgbIx], PiggybackedList[pgbIx+1:]...)
						}
					}
				}
			}
			mutex.Unlock()

		case "ACK":
			//fmt.Println("Receive ACK from :", msg.IpAddr, " --- MembershipList num : ", len(MembershipList))
			relativeIx := getRelativeIx(msg.IpAddr)
			if relativeIx != -1 {
				ACKtimers[relativeIx - 1].Reset(ACK_TIMEOUT)
			}

		case "Leave":
			/* Update MembershipList */
			InfoLog.Println("Receive leave meassage! Node IP:", msg.IpAddr, "leaves the group")
			//fmt.Println("Receive Leave from :", msg.IpAddr, " --- MembershipList num : ", len(MembershipList))
			mutex.Lock()
			targetIx := getIx(msg.IpAddr)
			if targetIx != -1 {
				MembershipList = append(MembershipList[:targetIx], MembershipList[targetIx+1:]...)
			}
			mutex.Unlock()
		}

	}
}

func getGrepLog() {
	fmt.Println("Input the keyword : ")
	reader := bufio.NewReader(os.Stdin)
	strMessage, err := reader.ReadString('\n')
	strMessage = strings.Replace(strMessage, "\n", "", -1)	// delete line feeds
	if err != nil {
		fmt.Println("err during read input:", err, ". Please try again.")
		return
	}

	var IPList = make([]string, len(MembershipList))
	for i, element := range MembershipList {
		IPList[i] = element.IpAddr
	}

	//fmt.Println("Your input is :", strMessage)
	mp1client.RunClientmain(strMessage, IPList)
}

func leaveMemship() {
	formatTimeStr := time.Unix(time.Now().Unix(), 0).Format("2006-01-02 15:04:05")
	pgyLst := make([]MemEntry, 0)
	var leaveMessage = MesInfoEntry{
		IpAddr:      LocalIp,
		Timestamp:   formatTimeStr,
		Type:        "Leave",
		PgyBackList: pgyLst,
	}
	receiverList := make([]string, 0)
	for _, member := range MembershipList {
		if member.IpAddr != LocalIp {
			receiverList = append(receiverList, member.IpAddr)
		}
	}
	sendMessage(leaveMessage, receiverList, MessagePort)
	InfoLog.Println("Current IP:", LocalIp, "is going to leave the group")
}

func listMembershipList() {
	fmt.Println("Id\tIP")
	for _, member := range MembershipList {
		fmt.Println(member.Id, "\t", member.IpAddr)
	}
}

func ProcessInput() {

	if LocalIp == JoinIp {
		go introAddNode()	/* only introducer run */
	}

	/* https://blog.csdn.net/zzzz_ing/article/details/53206096 */
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("a) list the membership list")
		fmt.Println("b) list id")
		fmt.Println("c) join the group")
		fmt.Println("d) leave the group")
		fmt.Println("e) grep the log")
		fmt.Println("Please input one option:")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("err during read input:", err, ". Please try again.")
			continue
		}
		input = strings.Replace(input, "\n", "", -1)	// delete line feeds
		switch input {
		case "a":
			if len(MembershipList) > 1 || LocalIp == JoinIp {
				listMembershipList()
			} else {
				fmt.Println("You have not joined the network yet !")
			}
		case "b":
			fmt.Println("Id is:", MembershipList[getIx(LocalIp)].Id)
			
		case "c":
			if LocalIp == JoinIp {
				fmt.Println("You are the introducer! You are already in the network!")
			} else if len(MembershipList) > 1 {
				fmt.Println("You are already in the network!")
			} else {
				go addToMemship()
				go listenToIntro()
			}
		case "d":
			if len(MembershipList) > 1 {
				leaveMemship()
			}
			os.Exit(0)
			/*
			initMembershipList()
			for i := 0; i < 3; i++ {
				ACKtimers[i].Stop()
			}
			*/
		case "e":
			getGrepLog()
		}
	}
}

func initMembershipList() {
	/* Init MembershipList */
	formatTimeStr := time.Unix(time.Now().Unix(), 0).Format("2006-01-02 15:04:05")
	ip := LocalIp
	entry := MemEntry{
		Id:     ip + "+" + formatTimeStr,
		IpAddr: ip,
	}
	MembershipList = append(MembershipList, entry)
}

func main() {

	initMembershipList()

	/* Init the log file */
	//file, err := os.OpenFile("../log/membership.log", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	file, err := os.OpenFile("../log/membership.log", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error during create log file", err)
		os.Exit(1)
	}
	defer file.Close()
	InfoLog = log.New(file, "[MemberInfo]", log.LstdFlags)
	ErrorLog = log.New(file, "[ErrorInfo]", log.LstdFlags)

	for i := 0; i < 3; i++ {
		ACKtimers[i] = time.NewTimer(ACK_TIMEOUT)
		ACKtimers[i].Stop()
	}

	/* Run the resident process */
	go listenMessages()
	go sendPing()
	for i := 1; i <= 3; i++ {
		go checkAck(i)	// relativeIx is 1~3
	}
	go mp1server.RunServermain()

	ProcessInput()
}
