package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	fileinfomap := &FileInfoMap{}
	fileinfomap.FileInfoMap = m.FileMetaMap
	return fileinfomap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// panic("todo")
	filename := fileMetaData.Filename
	newVersion := &Version{}
	if _, check := m.FileMetaMap[filename]; check {
		if fileMetaData.Version == m.FileMetaMap[filename].Version+1 {
			m.FileMetaMap[filename] = fileMetaData
			newVersion.Version = fileMetaData.Version
			return newVersion, nil
		} else {
			newVersion.Version = -1
			return newVersion, nil
		}
	} else {
		m.FileMetaMap[filename] = fileMetaData
		newVersion.Version = fileMetaData.Version
		return newVersion, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	// panic("todo")
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
