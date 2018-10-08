package main

import (
	"bufio"
	"strings"
	"encoding/json"
	"../mp1client"
)

func ProcessInput() {

	if LocalIp == JoinIp {
		go introAddNode()	/* only introducer run */
	}

	/* https://blog.csdn.net/zzzz_ing/article/details/53206096 */
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("#--------------------------------#")
		if len(MembershipList) > 1 || LocalIp == JoinIp {
			fmt.Println("# Satus -> Online")
		} else {
			fmt.Println("# Satus -> Offline")
		}
		fmt.Println("#--------------------------------#")
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
				time.Sleep(500 * time.Millisecond)
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

func listMembershipList() {
	fmt.Println("Id\tIP")
	for _, member := range MembershipList {
		fmt.Println(member.Id, "\t", member.IpAddr)
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
