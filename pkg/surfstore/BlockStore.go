package surfstore

import (
	context "context"
	"crypto/sha256"
	"encoding/hex"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// Retrieves a block indexed by hash value h
	// usage: b = GetBlock(h)
	return bs.BlockMap[blockHash.Hash], nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// panic("todo")
	hash := sha256.New()
	hash.Write(block.BlockData)
	hashBytes := hash.Sum(nil)
	hashcode := hex.EncodeToString(hashBytes)
	bs.BlockMap[hashcode] = block

	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// panic("todo")
	blockHashesOut := &BlockHashes{}
	for _, blockHash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[blockHash]; ok {
			blockHashesOut.Hashes = append(blockHashesOut.Hashes, blockHash)
		}
	}
	return blockHashesOut, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
