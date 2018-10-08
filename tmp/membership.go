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
						for i := 0; i < 3; i++ {
							resetTimerFlags[i] = 1
							ACKtimers[i].Reset(0)
						}
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
