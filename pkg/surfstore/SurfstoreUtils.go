package surfstore

import (
	"errors"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"math"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// panic("todo")

	// scan base directory
	baseFiles, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("read base directory faild")
	}

	dirfileMap := make(map[string]os.FileInfo)
	for _, fileOsInfo := range baseFiles {
		dirfileMap[fileOsInfo.Name()] = fileOsInfo
	}

	// check index.txt
	idxDir := client.BaseDir + "/" + "index.txt"
	_, idxPatherr := os.Stat(idxDir)
	if errors.Is(idxPatherr, fs.ErrNotExist) {
		idxfile, _ := os.Create(idxDir)
		defer idxfile.Close()
	}

	// format infors in index.txt
	oldFileInfoMap, loaderr := LoadMetaFromMetaFile(client.BaseDir)
	if loaderr != nil {
		log.Println("load local file meta data failed")
	}

	// scan files in local and update the local fileMetaDataMap for index.txt
	clientFileInfoMap, changeFlag := idxUpdate(client, dirfileMap, oldFileInfoMap)

	// get file from server
	serverFileInfoMap := make(map[string]*FileMetaData)
	getserverFileInfoMapErr := client.GetFileInfoMap(&serverFileInfoMap)
	if getserverFileInfoMapErr != nil {
		log.Println("get file infor map from server failed: ", getserverFileInfoMapErr)
	}

	// update file to server, update new version file to client
	// 1 file in both client & server
	// 2 file in client, not in server
	for filename, localFileMetaData := range clientFileInfoMap {
		// check if the server has this file
		if _, ok := serverFileInfoMap[filename]; ok {
			// server has the file
			serverFileMetaData := serverFileInfoMap[filename]
			// check if the local file is modified compared to the server file
			if localFileMetaData.Version == serverFileMetaData.Version && changeFlag[filename] == "unmodified" {
				// the local file is identical to that in server
				continue
			} else if localFileMetaData.Version > serverFileMetaData.Version {
				// the server file is behind the client file
				serverFileInfoMap[filename] = updateServerFileInfoMap(localFileMetaData)
				err := upload(client, filename, localFileMetaData)
				if err != nil {
					log.Println("upload failed")
				}
			} else {
				// the client file is old, update it
				clientFileInfoMap[filename] = updateClientFileInfoMap(serverFileMetaData)
				// download file into the client
				download(client, filename, serverFileMetaData)
			}

		} else {
			// server does not have the file --> update it to the server file info map
			serverFileInfoMap[filename] = updateServerFileInfoMap(localFileMetaData)
			err := upload(client, filename, localFileMetaData)
			if err != nil {
				log.Println("upload failed")
			}
		}
	}

	// download new file in the server && not in client
	for filename, serverFileMetaData := range serverFileInfoMap {
		// check if file in the server exists in client
		// download files not in the client
		if _, ok := clientFileInfoMap[filename]; !ok {
			clientFileInfoMap[filename] = serverFileMetaData
			download(client, filename, serverFileMetaData)
		}
	}

	// update index.txt with clientFileInfoMap
	updidxerr := WriteMetaFile(clientFileInfoMap, client.BaseDir)
	if updidxerr != nil {
		log.Println("update index.txt failed")
	}

}

func idxUpdate(client RPCClient, dirFileInfoMap map[string]os.FileInfo, oldFileInfoMap map[string]*FileMetaData) (returnmap map[string]*FileMetaData, returnFlag map[string]string) {
	newFileInfoMap := make(map[string]*FileMetaData)
	changeFlag := make(map[string]string)
	// handle files in the base directory: new to index.txt or already exists in index.txt(modified or unchanged)
	for filename, fileosInfo := range dirFileInfoMap {
		// get content of the files from the base directory
		if filename == "index.txt" {
			continue
		}

		// generate hashlist for the file content
		dirfilecontentHashlist := genHashlist(client, filename, fileosInfo) // [h0 h1 h2 ... hn]
		var newfileMetaData *FileMetaData

		// check if this file exists in oldFileInforMap(modified/unchanged)
		if oldfileMetaData, check := oldFileInfoMap[filename]; check {
			// if exists
			//check if it's modified
			changeflg := checkChange(dirfilecontentHashlist, oldfileMetaData.BlockHashList) // true: changed | false: unchanged
			if changeflg {
				// if modified --> newfilemetadata: version+1
				changeFlag[filename] = "modified"
				// modifiedfileMetaData := &FileMetaData{}
				// modifiedfileMetaData.Filename = oldfileMetaData.Filename
				// modifiedfileMetaData.Version = oldfileMetaData.Version + 1
				//// modifiedfileMetaData.BlockHashList = dirfilecontentHashlist
				// for i, hashcode := range dirfilecontentHashlist {
				// 	modifiedfileMetaData.BlockHashList[i] = hashcode
				// }
				// newFileInfoMap[filename] = modifiedfileMetaData

				newfileMetaData.Filename = oldfileMetaData.Filename
				newfileMetaData.Version = oldfileMetaData.Version + 1
				for i, hashcode := range dirfilecontentHashlist {
					newfileMetaData.BlockHashList[i] = hashcode
				}
			} else {
				// if not modified --> add data to newfilemetadatamap
				changeFlag[filename] = "unmodified"
				// newFileInfoMap[filename] = oldfileMetaData

				// unmodifiedfileMetaData := &FileMetaData{}
				// unmodifiedfileMetaData.Filename = oldfileMetaData.Filename
				// unmodifiedfileMetaData.Version = oldfileMetaData.Version
				// for i, hashcode := range dirfilecontentHashlist {
				// 	unmodifiedfileMetaData.BlockHashList[i] = hashcode
				// }
				// newFileInfoMap[filename] = unmodifiedfileMetaData

				newfileMetaData.Filename = oldfileMetaData.Filename
				newfileMetaData.Version = oldfileMetaData.Version
				for i, hashcode := range dirfilecontentHashlist {
					newfileMetaData.BlockHashList[i] = hashcode
				}
			}
		} else {
			// if not exists(new file)
			changeFlag[filename] = "newfile"
			// newfileMetaData := &FileMetaData{}
			// newfileMetaData.Filename = filename
			// newfileMetaData.Version = 1
			// newfileMetaData.BlockHashList = dirfilecontentHashlist
			// newFileInfoMap[filename] = newfileMetaData

			newfileMetaData.Filename = filename
			newfileMetaData.Version = 1
			for i, hashcode := range dirfilecontentHashlist {
				newfileMetaData.BlockHashList[i] = hashcode
			}

		}
		newFileInfoMap[filename] = newfileMetaData
	}

	// handle files does not exists in the index.txt(deleted)
	for filename, oldfilemetadata := range oldFileInfoMap {
		var newfileMetaData *FileMetaData
		if _, ok := dirFileInfoMap[filename]; !ok {
			// for files recorded in index.txt but not exists in client --> file is deleted
			if len(oldfilemetadata.BlockHashList) == 1 && oldfilemetadata.BlockHashList[0] == "0" {
				// recorded as deleted
				changeFlag[filename] = "unmodified"
				newfileMetaData.Filename = oldfilemetadata.Filename
				newfileMetaData.Version = oldfilemetadata.Version
				for i, hashcode := range oldfilemetadata.BlockHashList {
					newfileMetaData.BlockHashList[i] = hashcode
				}
			} else {
				// deleted in client but exist in index.txt --> update newfilemetadata marked as deleted
				changeFlag[filename] = "deleted"
				newfileMetaData.Filename = oldfilemetadata.Filename
				newfileMetaData.Version = oldfilemetadata.Version + 1
				newfileMetaData.BlockHashList = make([]string, 1)
				newfileMetaData.BlockHashList[0] = "0"
			}
			newFileInfoMap[filename] = newfileMetaData
		}
	}

	// handle files does not exists in the index.txt(deleted)
	// handleDelFiles(&newFileInfoMap, dirFileInfoMap, oldFileInfoMap, &changeFlag)

	return newFileInfoMap, changeFlag
}

func genHashlist(client RPCClient, filename string, fileosInfo os.FileInfo) (hashlist []string) {
	filecontent, err := os.Open(client.BaseDir + "/" + filename)
	if err != nil {
		log.Printf("open file %s failed\n", filename)
	}
	filesize := fileosInfo.Size()
	numofBlock := int(math.Ceil(float64(filesize) / float64(client.BlockSize)))

	hashlist = make([]string, numofBlock)

	for i := 0; i < numofBlock; i++ {
		// generate hashcode for each block
		buffer := make([]byte, client.BlockSize)
		hashbytes, err := filecontent.Read(buffer)
		if err != nil {
			log.Println("read file bytes error")
		}

		buffer = buffer[:hashbytes]
		hashcode := GetBlockHashString(buffer)
		hashlist[i] = hashcode
	}

	return hashlist
}

func checkChange(filehashlist []string, oldidxfilehashlist []string) bool {
	if len(filehashlist) == len(oldidxfilehashlist) {
		for i := 0; i < len(filehashlist); i++ {
			if oldidxfilehashlist[i] != filehashlist[i] {
				return true
			}
		}
		return false
	}
	return true
}

// func handleDelFiles(newFileInfoMap *map[string]*FileMetaData, dirFileInfoMap map[string]os.FileInfo, idxFileInfoMap map[string]*FileMetaData, changeFlag *map[string]string) {
// 	for filename, fileMetaData := range idxFileInfoMap {
// 		// check if the files in the index.txt has been deleted
// 		if _, ok := dirFileInfoMap[filename]; !ok {
// 			// if deleted
// 			if len(fileMetaData.BlockHashList) == 1 && fileMetaData.BlockHashList[0] == "0" {
// 				// if it has been recorded as deleted in the index.txt
// 				(*changeFlag)[filename] = "unmodified"
// 				newfileMetaData := &FileMetaData{}
// 				newfileMetaData.Filename = filename
// 				newfileMetaData.Version = fileMetaData.Version
// 				newfileMetaData.BlockHashList = fileMetaData.BlockHashList
// 				(*newFileInfoMap)[filename] = newfileMetaData
// 			} else {
// 				// if it's deleted in base dir but not deleted in the index.txt
// 				(*changeFlag)[filename] = "modified"
// 				zerohash := make([]string, 1)
// 				zerohash[0] = "0"
// 				newfileMetaData := &FileMetaData{}
// 				newfileMetaData.Filename = filename
// 				newfileMetaData.Version = fileMetaData.Version + 1
// 				newfileMetaData.BlockHashList = zerohash
// 				(*newFileInfoMap)[filename] = newfileMetaData
// 			}
// 		}
// 	}
// }

// updateClientFileInfoMap(client, &localFileMetaData, serverFileMetaData)

func updateClientFileInfoMap(serverFileMetaData *FileMetaData) (newClientFileMetaData *FileMetaData) {
	// check if the client file should be deleted
	newClientFileMetaData = &FileMetaData{}
	newClientFileMetaData.Filename = serverFileMetaData.Filename
	newClientFileMetaData.Version = serverFileMetaData.Version
	newClientFileMetaData.BlockHashList = serverFileMetaData.BlockHashList
	return newClientFileMetaData
}

func updateServerFileInfoMap(localFileMetaData *FileMetaData) (newServerFileMetaData *FileMetaData) {
	newServerFileMetaData = &FileMetaData{}
	newServerFileMetaData.Filename = localFileMetaData.Filename
	newServerFileMetaData.Version = localFileMetaData.Version
	newServerFileMetaData.BlockHashList = localFileMetaData.BlockHashList
	return newServerFileMetaData
}

// download(client, filename, serverFileMetaData)
func download(client RPCClient, filename string, serverFileMetaData *FileMetaData) {
	filepath := client.BaseDir + "/" + filename
	// check if the file exists in client base dir
	_, idxPatherr := os.Stat(filepath)
	if errors.Is(idxPatherr, fs.ErrNotExist) {
		idxfile, _ := os.Create(filepath)
		defer idxfile.Close()
	} else {
		// clear up the file to write new data
		os.Truncate(filepath, 0)
	}

	// check if it should be deleted
	if len(serverFileMetaData.BlockHashList) == 1 && serverFileMetaData.BlockHashList[0] == "0" {
		err := os.Remove(filepath)
		if err != nil {
			log.Println("Cannot remove file: ", err)
		}
	}

	// write data to file
	file, _ := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755) // Add file access mode.
	defer file.Close()

	for _, hashcode := range serverFileMetaData.BlockHashList {
		var blockData Block
		var blockaddr string
		getBlockAddrErr := client.GetBlockStoreAddr(&blockaddr)
		if getBlockAddrErr != nil {
			log.Println("get block address failed")
		}
		getBlockErr := client.GetBlock(hashcode, blockaddr, &blockData)
		if getBlockErr != nil {
			log.Println("get block failed")
		}
		data := string(blockData.BlockData)
		_, wrterr := io.WriteString(file, data)
		if wrterr != nil {
			log.Println("write data to file failed")
		}
	}

}

// upload(filename, localFileMetaData)
func upload(client RPCClient, filename string, clientFileMetaData *FileMetaData) error {
	// upload new (version) client files to server
	var err error
	filepath := client.BaseDir + "/" + filename
	file, err := os.Stat(filepath)
	if errors.Is(err, fs.ErrNotExist) {
		// delete operation
		err = client.UpdateFile(clientFileMetaData, &clientFileMetaData.Version)
		if err != nil {
			log.Println("update deleted file failed")
		}
		return err
	}

	filectx, err := os.Open(filepath)
	if err != nil {
		log.Println("open file error")
	}
	defer filectx.Close()

	numofblock := int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))

	// generate & put block
	for i := 0; i < numofblock; i++ {
		var block Block
		block.BlockData = make([]byte, client.BlockSize)
		bytelength, err := filectx.Read(block.BlockData)
		if err != nil && err != io.EOF {
			log.Println("read file failed")
		}
		block.BlockSize = int32(bytelength)
		block.BlockData = block.BlockData[:block.BlockSize]

		var succ bool
		var blockaddr string
		err = client.GetBlockStoreAddr(&blockaddr)
		if err != nil {
			log.Println("get block address failed")
		}
		err = client.PutBlock(&block, blockaddr, &succ)
		if err != nil {
			log.Println("put block failed")
		}
	}
	return err

}
