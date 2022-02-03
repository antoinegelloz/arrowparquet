package arrowparquet

import (
	"fmt"
	"github.com/apache/arrow/go/v7/parquet/file"
	"github.com/apache/arrow/go/v7/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestArrowParquet(t *testing.T) {
	t.Run("pass", func(t *testing.T) {
		test(t, "pass.parquet")
	})
	t.Run("fail", func(t *testing.T) {
		test(t, "fail.parquet")
	})
}

func test(t *testing.T, path string) {
	parquetFile, err := file.OpenParquetFile(path, false)
	assert.NoError(t, err)
	defer parquetFile.Close()

	const arrowSchemaKey = "ARROW:schema"
	serialized := parquetFile.MetaData().KeyValueMetadata().FindValue(arrowSchemaKey)
	fmt.Printf("%s: ARROW:schema\n%s\n", path, *serialized)

	fmt.Printf("input byte 811 |%s|\n", (*serialized)[811:])

	_, err = pqarrow.NewSchemaManifest(
		parquetFile.MetaData().Schema,
		parquetFile.MetaData().KeyValueMetadata(), &pqarrow.ArrowReadProperties{})
	assert.NoError(t, err)
}
