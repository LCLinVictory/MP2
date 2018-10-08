package main

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
			for i := 0; i < 3; i++ {
				resetTimerFlags[i] = 1
				ACKtimers[i].Reset(0)
			}
			MembershipList = append(MembershipList, entry)
			InfoLog.Println("Member ID:", entry.Id, " joins into my group")
			mutex.Unlock()
			broadCast(entry)
		}
	}
}
