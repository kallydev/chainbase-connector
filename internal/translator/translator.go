package translator

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/kallydev/chainbase-connector/internal/chainbase"
)

func Translate(data chainbase.DataWarehouseData) ([]byte, error) {
	var buffer proto.Buffer

	buffer.PutInt(len(data.Meta))
	buffer.PutInt(data.Rows)

	for _, column := range data.Meta {
		buffer.PutString(column.Name)
		buffer.PutString(column.Type)

		for _, row := range data.Result {
			switch columnType := proto.ColumnType(column.Type); columnType {
			case
				proto.ColumnTypeInt8, proto.ColumnTypeInt16, proto.ColumnTypeInt32,
				proto.ColumnTypeUInt8, proto.ColumnTypeUInt16, proto.ColumnTypeUInt32,
				proto.ColumnTypeFloat32, proto.ColumnTypeFloat64:
				value, ok := row[column.Name].(float64)
				if !ok {
					return nil, fmt.Errorf("invalid type: %T", row[column.Name])
				}

				switch columnType {
				case proto.ColumnTypeInt8:
					buffer.PutInt8(int8(value))
				case proto.ColumnTypeInt16:
					buffer.PutInt16(int16(value))
				case proto.ColumnTypeInt32:
					buffer.PutInt32(int32(value))
				case proto.ColumnTypeUInt8:
					buffer.PutUInt8(uint8(value))
				case proto.ColumnTypeUInt16:
					buffer.PutUInt16(uint16(value))
				case proto.ColumnTypeUInt32:
					buffer.PutUInt32(uint32(value))
				case proto.ColumnTypeFloat32:
					buffer.PutFloat32(float32(value))
				case proto.ColumnTypeFloat64:
					buffer.PutFloat64(value)
				default:
					return nil, fmt.Errorf("unsupport type: %s", column.Type)
				}
			case
				proto.ColumnTypeInt64, proto.ColumnTypeInt128, proto.ColumnTypeInt256,
				proto.ColumnTypeUInt64, proto.ColumnTypeUInt128, proto.ColumnTypeUInt256,
				proto.ColumnTypeString,
				proto.ColumnTypeDateTime, proto.ColumnTypeDate:
				value, ok := row[column.Name].(string)
				if !ok {
					return nil, fmt.Errorf("invalid type: %T", row[column.Name])
				}

				switch columnType {
				case proto.ColumnTypeInt64:
					value, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("parse int64: %w", err)
					}

					buffer.PutInt64(value)
				case proto.ColumnTypeUInt64:
					value, err := strconv.ParseUint(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("parse uint64: %w", err)
					}

					buffer.PutUInt64(value)
				case proto.ColumnTypeString:
					buffer.PutString(value)
				case proto.ColumnTypeDateTime:
					dateTime, err := time.Parse(time.DateTime, value)
					if err != nil {
						return nil, fmt.Errorf("parse datetime: %w", err)
					}

					buffer.PutUInt32(uint32(dateTime.Unix()))
				case proto.ColumnTypeDate:
					value, err := time.Parse(proto.DateLayout, value)
					if err != nil {
						return nil, fmt.Errorf("parse date: %w", err)
					}

					buffer.PutUInt16(uint16(value.Unix() / (60 * 60 * 24)))
				default:
					return nil, fmt.Errorf("unsupport type: %s", column.Type)
				}
			case proto.ColumnTypeBool:
				value, ok := row[column.Name].(bool)
				if !ok {
					return nil, fmt.Errorf("invalid type: %T", row[column.Name])
				}

				buffer.PutBool(value)
			default:
				return nil, fmt.Errorf("unsupport type: %s", column.Type)
			}
		}
	}

	return buffer.Buf, nil
}
