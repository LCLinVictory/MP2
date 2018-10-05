package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
	"sync"
	"bufio"
	"strings"
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
	MIN_LIST_SIZE	= 4
	PING_PERIOD		= time.Second * 1 
	ACK_TIMEOUT		= time.Millisecond * 2000
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
)

// https://blog.csdn.net/yxys01/article/details/78054757
// get the local ip address
func getLocalIp() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println("can not get the local ipAddr:", err)
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
			fmt.Println("Can not resolve the address:", err)
			os.Exit(1)
		}

		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			fmt.Println("Can not dial during sendMessage:", err)
			os.Exit(1)
		}
		defer conn.Close()
		err = json.NewEncoder(&buf).Encode(mesInfo)
		if err != nil {
			fmt.Println("can not encode into json:", err)
			os.Exit(1)
		}
		_, err = conn.Write(buf.Bytes())
		//fmt.Println("In sendMessage: Write message =", string(buf.Bytes()))
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
		fmt.Println("Can not listenUDP during listenToIntro:", err)
		os.Exit(1)
	}
	defer conn.Close()
	//fmt.Println("test!!!")
	for {
		entryList := make([]MemEntry, 0)
		buf := make([]byte, 1024)
		n, _, err := conn.ReadFromUDP(buf)
		err = json.NewDecoder(bytes.NewReader(buf[:n])).Decode(&entryList)
		//fmt.Println("IN listenToIntro:", entryList)
		if err != nil {
			fmt.Println("Can not decode during listenToIntro:", err)
			os.Exit(1)
		}
		if len(entryList) == 1 {
			MembershipList = append(MembershipList, entryList[0])
		} else {
			MembershipList = entryList
		}
		fmt.Println("MembershipList in listenToIntro Now:", MembershipList)
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
				fmt.Println("Can not resolve addr during broadCast:", err)
				os.Exit(1)
			}
			conn, err := net.DialUDP("udp", nil, addr)
			if err != nil {
				fmt.Println("Can not dialUDP during broadCast:", err)
				os.Exit(1)
			}
			defer conn.Close()
			if member.IpAddr != entry.IpAddr {
				err = json.NewEncoder(&bufOther).Encode(MemberNew)
				if err != nil {
					fmt.Println("Can not encodeToOther during broadcast:", err)
					os.Exit(1)
				}
				conn.Write(bufOther.Bytes())
				fmt.Println("In broadcast!", string(bufOther.Bytes()))
			} else {
				err = json.NewEncoder(&bufNew).Encode(MembershipList)
				if err != nil {
					fmt.Println("Can not encode during broadCastToNew:", err)
					os.Exit(1)
				}
				conn.Write(bufNew.Bytes())
				fmt.Println("In broadcast!", string(bufNew.Bytes()))
			}
		}
	}
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
		fmt.Println("Can not listen:", err)
		os.Exit(1)
	}
	defer conn.Close()

	for {
		Mentry := MesInfoEntry{}
		buf := make([]byte, 1024)
		n, _, err := conn.ReadFromUDP(buf)
		err = json.NewDecoder(bytes.NewReader(buf[:n])).Decode(&Mentry)
		if err != nil {
			fmt.Println("Json decode failed:", err)
			os.Exit(1)
		}
		fmt.Println("IntroAddNode:", Mentry)
		formatTimeStr := time.Unix(time.Now().Unix(), 0).Format("2006-01-02 15:04:05")
		ip := Mentry.IpAddr
		entry := MemEntry{
			Id:     ip + "+" + formatTimeStr,
			IpAddr: ip,
		}
		/*check timestamp pass*/
		MembershipList = append(MembershipList, entry)
		broadCast(entry)
	}
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
 * This function sends Ping messages to next three successive neighbours every PING_PERIOD
 */
func sendPing() {
	for {
		MemshipNum := len(MembershipList)
		if MemshipNum >= MIN_LIST_SIZE {
			var receiverList = make([]string, 3)
			formatTimeStr := time.Unix(time.Now().Unix(), 0).Format("2006-01-02 15:04:05")
			piggyList := make([]MemEntry, 0)
			if len(PiggybackedList) > 0 {
				piggyList = PiggybackedList
			}
			JoinMessage := MesInfoEntry{
				IpAddr:  		LocalIp,
				Timestamp:		formatTimeStr,
				Type:			"PING",
				PgyBackList:	piggyList,
			}
			receiverList[0] = MembershipList[(getIx(LocalIp)+1)%MemshipNum].IpAddr
			receiverList[1] = MembershipList[(getIx(LocalIp)+2)%MemshipNum].IpAddr
			receiverList[2] = MembershipList[(getIx(LocalIp)+3)%MemshipNum].IpAddr
			sendMessage(JoinMessage, receiverList, MessagePort)
		}
		time.Sleep(PING_PERIOD)
	}
}

/*
 * Function to give the relative location of the host with respect to the current node in the ML
 */
func getRelativeIx(targetIP string) int {
	localIx := getIx(LocalIp)
	MemshipNum := len(MembershipList)
	if strings.Compare(MembershipList[(localIx+1)%MemshipNum].IpAddr, targetIP) == 0 {
		return 1
	} else if strings.Compare(MembershipList[(localIx+2)%MemshipNum].IpAddr, targetIP) == 0 {
		return 2
	} else if strings.Compare(MembershipList[(localIx+3)%MemshipNum].IpAddr, targetIP) == 0 {
		return 3
	}
	return -1
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

	//ACKtimers[relativeIx-1] = time.NewTimer(ACK_TIMEOUT)
	ACKtimers[relativeIx-1].Reset(ACK_TIMEOUT)	// !!!
	<-ACKtimers[relativeIx-1].C 	// waiting to be triggered

	mutex.Lock()
	if len(MembershipList) >= MIN_LIST_SIZE && getRelativeIx(relativeIP) == relativeIx && resetTimerFlags[relativeIx-1] != 1 {
		/* Failure detected first time */
		fmt.Println("Failure detected at IpAddr : ", relativeIP)
		targetIx := getIx(relativeIP)
		node := MemEntry{MembershipList[targetIx].Id, relativeIP}
		PiggybackedList = append(PiggybackedList, node)
		/* Update MembershipList */
		MembershipList = append(MembershipList[:targetIx], MembershipList[targetIx+1:]...)
		
	}
	// None of of the Events should be updating the MembershipList , only then this condition would be set.
	// Reset all the other timers (which the current node is monitoring) as well if the above condition is met
	if resetTimerFlags[relativeIx-1] == 0 {
		fmt.Println("Force stopping other timers :", string(relativeIx))
		for i := 1; i < 3; i++ {
			resetTimerFlags[i] = 1
			ACKtimers[i].Reset(0)		// !!!
		}
	} else {
		resetTimerFlags[relativeIx-1] = 0
	}

	mutex.Unlock()
	go checkAck(relativeIx)

}

/*
 * Listen to messages on UDP port from other nodes and take appropriate action. Possible message types are
 * PING, ACK, ...
 */
func listenMessages() {
	addr, err := net.ResolveUDPAddr("udp", ":"+MessagePort)
	if err != nil {
		fmt.Println("Can not resolve addr during listenMessages:", err)
		os.Exit(1)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Println("Can not listenUDP during listenMessages:", err)
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
			fmt.Println("Can not decode during listenMessages:", err)
			os.Exit(1)
		}
		switch msg.Type {
		case "PING":
			var receiverList = make([]string, 1)
			formatTimeStr := time.Unix(time.Now().Unix(), 0).Format("2006-01-02 15:04:05")
			piggyList := make([]MemEntry, 0)
			JoinMessage := MesInfoEntry{
				IpAddr:  		LocalIp,
				Timestamp:		formatTimeStr,
				Type:			"ACK",
				PgyBackList:	piggyList,
			}
			receiverList[0] = msg.IpAddr
			sendMessage(JoinMessage, receiverList, MessagePort)

			/* Find target IpAddr in MembershipList */
			mutex.Lock()
			if len(msg.PgyBackList) > 0 {
				for _, member := range msg.PgyBackList {
					targetIx := getIx(member.IpAddr)
					if ( targetIx != -1 && member.Id == MembershipList[targetIx].Id )  {
						/* Update MembershipList */
						MembershipList = append(MembershipList[:targetIx], MembershipList[targetIx+1:]...)
					}
					var pgbIx = -1
					for i, element := range PiggybackedList {
						if member.Id == element.Id {
							pgbIx = i
							break
						}
					}
					if pgbIx != -1 {
						/* Update PiggybackedList */
						PiggybackedList = append(PiggybackedList[:pgbIx], PiggybackedList[pgbIx+1:]...)
					}
				}
			}
			mutex.Unlock()

		case "ACK":
			fmt.Println("Receive ACK from :", msg.IpAddr)
			relativeIx := getRelativeIx(msg.IpAddr)
			if relativeIx != -1 {
				ACKtimers[relativeIx - 1].Reset(ACK_TIMEOUT)
			}
		}

	}
}


func ProcessInput() {

	// https://blog.csdn.net/zzzz_ing/article/details/53206096
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("a) list the membership list")
		fmt.Println("b) list id")
		fmt.Println("c) join the group")
		fmt.Println("d) leave the group")
		fmt.Println("Please input one option:")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("err during read input:", err, ". Please try again.")
			continue
		}
		input = strings.Replace(input, "\n", "", -1)
		switch input {
		case "a":
			listMembershipList()
		case "b":
			getID()
		case "c":
			addToMemship()
		case "d":
			leaveMemship()
		}
	}
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
}

func getID() {
	for _, member := range MembershipList {
		if member.IpAddr == LocalIp {
			fmt.Println("Id is:", member.Id)
		}
	}
}

func listMembershipList() {
	fmt.Println("Id\tIP")
	for _, member := range MembershipList {
		fmt.Println(member.Id, "\t", member.IpAddr)
	}
}


func main() {
	formatTimeStr := time.Unix(time.Now().Unix(), 0).Format("2006-01-02 15:04:05")
	ip := LocalIp
	entry := MemEntry{
		Id:     ip + "+" + formatTimeStr,
		IpAddr: ip,
	}
	/* Init MembershipList */
	MembershipList = append(MembershipList, entry)

	ACKtimers[0] = time.NewTimer(ACK_TIMEOUT)
	ACKtimers[1] = time.NewTimer(ACK_TIMEOUT)
	ACKtimers[2] = time.NewTimer(ACK_TIMEOUT)
	ACKtimers[0].Stop()
	ACKtimers[1].Stop()
	ACKtimers[2].Stop()

	go listenMessages()
	go sendPing()
	go checkAck(1)
	go checkAck(2)
	go checkAck(3)
	if LocalIp == JoinIp {
		introAddNode()
	} else {
		go addToMemship()
		listenToIntro()
	}

}
