package main

import (
	"strings"
	"math/rand"
	"encoding/json"
)

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
