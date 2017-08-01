package org.knime.python2.serde.arrow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableBigIntVector;
import org.apache.arrow.vector.NullableBitVector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.NullableIntVector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ValueVector.Accessor;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.ArrayUtils;
import org.knime.python2.extensions.serializationlibrary.SerializationOptions;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.SerializationLibrary;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableCreator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.CellImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.RowImpl;
import org.knime.python2.extensions.serializationlibrary.interfaces.impl.TableSpecImpl;
import org.knime.python2.serde.arrow.libraryextensions.ArrowBatchWriter;

public class ArrowSerializationLibrary implements SerializationLibrary {

    private JsonObjectBuilder createColumnMetadataBuilder(String name, String pandasType, String numpyType, 
            Type knimeType, String serializer) {
        JsonObjectBuilder colMetadataBuilder = Json.createObjectBuilder();
        colMetadataBuilder.add("name", name);
        colMetadataBuilder.add("pandas_type", pandasType);
        colMetadataBuilder.add("numpy_type", numpyType);
        JsonObjectBuilder knimeMetadataBuilder = Json.createObjectBuilder();
        knimeMetadataBuilder.add("type_id", knimeType.getId());
        knimeMetadataBuilder.add("serializer_id", serializer);
        colMetadataBuilder.add("metadata", knimeMetadataBuilder);
        return colMetadataBuilder;
    }
    
    /**
     * Rounds down the provided value to the nearest power of two.
     *
     * @param val An integer value.
     * @return The closest power of two of that value.
     */
    static int nextSmallerPowerOfTwo(int val) {
      int highestBit = Integer.highestOneBit(val);
      if (highestBit == val) {
        return val;
      } else {
        return highestBit;
      }
    }
    
    /*Note: should be a power of 2*/
    private static int ASSUMED_ROWID_VAL_BYTE_SIZE = 4;
    /*Note: should be a power of 2*/
    private static int ASSUMED_STRING_VAL_BYTE_SIZE = 64;
    /*Note: should be a power of 2*/
    private static int ASSUMED_BYTES_VAL_BYTE_SIZE = 32;
    private static long FIXED_BATCH_BYTE_SIZE = 5 * 1024 * 1024;
    
    @Override
    public byte[] tableToBytes(TableIterator tableIterator, SerializationOptions serializationOptions) {
        String path = System.getProperty("java.io.tmpdir") + File.separator + "memory_mapped.dat";
        try {
            FileChannel fc = new RandomAccessFile(new File(path), "rw").getChannel();
            return tableToBytesDynamic(tableIterator, serializationOptions, fc, path);

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }
    
    private byte[] tableToBytesDynamic(TableIterator tableIterator, SerializationOptions serializationOptions,
            FileChannel fc, String path) throws IOException {
            // Get metadata
            final String INDEX_COL_NAME = "__index_level_0__";
            JsonObjectBuilder metadataBuilder = Json.createObjectBuilder();
            TableSpec spec = tableIterator.getTableSpec();
            List<FieldVector> vecs = new ArrayList<FieldVector>();
            List<Field> fields = new ArrayList<Field>();
            JsonArrayBuilder icBuilder = Json.createArrayBuilder();
            RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
            icBuilder.add(INDEX_COL_NAME);
            metadataBuilder.add("index_columns", icBuilder);
            JsonArrayBuilder colBuilder = Json.createArrayBuilder();
            // Row ids
            JsonObjectBuilder rowIdBuilder = createColumnMetadataBuilder(INDEX_COL_NAME, "unicode", "object",
                    Type.STRING, "");
            colBuilder.add(rowIdBuilder);
            NullableVarCharVector rowIdVector = new NullableVarCharVector(INDEX_COL_NAME, rootAllocator);
            rowIdVector.allocateNew(ASSUMED_ROWID_VAL_BYTE_SIZE * tableIterator.getNumberRemainingRows(),
                    tableIterator.getNumberRemainingRows());
            vecs.add(rowIdVector);
            fields.add(rowIdVector.getField());

            // Create FieldVectors and metadata
            for (int i = 0; i < spec.getNumberColumns(); i++) {
                JsonObjectBuilder colMetadataBuilder;
                FieldVector vec;
                switch (spec.getColumnTypes()[i]) {
                case BOOLEAN:
                    colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "bool", "object",
                            Type.BOOLEAN, "");
                    // Allocate vector for column
                    NullableBitVector bovec = new NullableBitVector(spec.getColumnNames()[i], rootAllocator);
                    bovec.allocateNew(tableIterator.getNumberRemainingRows());
                    vec = bovec;
                    break;
                case INTEGER:
                    colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "int", "int32",
                            Type.INTEGER, "");
                    // Allocate vector for column
                    NullableIntVector ivec = new NullableIntVector(spec.getColumnNames()[i], rootAllocator);
                    ivec.allocateNew(tableIterator.getNumberRemainingRows());
                    vec = ivec;
                    break;
                case LONG:
                    colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "int", "int64",
                            Type.LONG, "");
                    // Allocate vector for column
                    NullableBigIntVector lvec = new NullableBigIntVector(spec.getColumnNames()[i], rootAllocator);
                    lvec.allocateNew(tableIterator.getNumberRemainingRows());
                    vec = lvec;
                    break;
                case DOUBLE:
                    colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "int", "float64",
                            Type.DOUBLE, "");
                    // Allocate vector for column
                    NullableFloat8Vector dvec = new NullableFloat8Vector(spec.getColumnNames()[i], rootAllocator);
                    dvec.allocateNew(tableIterator.getNumberRemainingRows());
                    vec = dvec;
                    break;
                case STRING:
                    colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "unicode", "object",
                            Type.STRING, "");
                    NullableVarCharVector vvec = new NullableVarCharVector(spec.getColumnNames()[i], rootAllocator);
                    vvec.allocateNew(ASSUMED_STRING_VAL_BYTE_SIZE * tableIterator.getNumberRemainingRows(),
                            tableIterator.getNumberRemainingRows());
                    vec = vvec;
                    break;
                case BYTES:
                    colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object",
                            Type.BYTES, spec.getColumnSerializers().get(spec.getColumnNames()[i]));
                    NullableVarBinaryVector bvec = new NullableVarBinaryVector(spec.getColumnNames()[i], rootAllocator);
                    bvec.allocateNew(ASSUMED_BYTES_VAL_BYTE_SIZE * tableIterator.getNumberRemainingRows(),
                            tableIterator.getNumberRemainingRows());
                    vec = bvec;
                    break;
                case INTEGER_LIST:
                    colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object",
                            Type.INTEGER_LIST, "");
                    NullableVarBinaryVector ilsvec = new NullableVarBinaryVector(spec.getColumnNames()[i], rootAllocator);
                    ilsvec.allocateNew(ASSUMED_BYTES_VAL_BYTE_SIZE * tableIterator.getNumberRemainingRows(),
                            tableIterator.getNumberRemainingRows());
                    vec = ilsvec;
                    break;
                case INTEGER_SET:
                    colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object",
                            Type.INTEGER_SET, "");
                    NullableVarBinaryVector isetvec = new NullableVarBinaryVector(spec.getColumnNames()[i], rootAllocator);
                    isetvec.allocateNew(ASSUMED_BYTES_VAL_BYTE_SIZE * tableIterator.getNumberRemainingRows(),
                            tableIterator.getNumberRemainingRows());
                    vec = isetvec;
                    break;
                default:
                    throw new IllegalStateException(
                            "Serialization is not implemented for type: " + spec.getColumnTypes()[i].name());
                }
                vecs.add(vec);
                fields.add(vec.getField());
                colBuilder.add(colMetadataBuilder);
            }
            metadataBuilder.add("columns", colBuilder);

            int ctr = 0;
            int[] val_length = new int[vecs.size()];
            while (tableIterator.hasNext()) {
                Row row = tableIterator.next();
                byte[] bRowKey = row.getRowKey().getBytes("UTF-8");
                val_length[0] += bRowKey.length;
                while (val_length[0] > ((NullableVarCharVector) vecs.get(0)).getByteCapacity()) {
                    ((NullableVarCharVector) vecs.get(0)).reAlloc();
                }
                ((NullableVarCharVector.Mutator) vecs.get(0).getMutator()).set(ctr, bRowKey);
                ((NullableVarCharVector.Mutator) vecs.get(0).getMutator()).setValueCount(ctr + 1);
                for (int i = 0; i < spec.getNumberColumns(); i++) {
                    switch (spec.getColumnTypes()[i]) {
                    case BOOLEAN:
                        if (row.getCell(i).isMissing()) {
                            ((NullableBitVector.Mutator) vecs.get(i + 1).getMutator()).setNull(ctr);
                        } else {
                            val_length[i + 1]++;
                            ((NullableBitVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr,
                                    row.getCell(i).getBooleanValue().booleanValue() ? 1 : 0);
                        }
                        ((NullableBitVector.Mutator) vecs.get(i + 1).getMutator()).setValueCount(ctr + 1);
                        break;
                    case INTEGER:
                        if (row.getCell(i).isMissing()) {
                            if (serializationOptions.getConvertMissingToPython()) {
                                val_length[i + 1]++;
                                ((NullableIntVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr,
                                        (int) serializationOptions.getSentinelForType(Type.INTEGER));
                            } else {
                                ((NullableIntVector.Mutator) vecs.get(i + 1).getMutator()).setNull(ctr);
                            }
                        } else {
                            val_length[i + 1]++;
                            ((NullableIntVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr,
                                    row.getCell(i).getIntegerValue().intValue());
                        }
                        ((NullableIntVector.Mutator) vecs.get(i + 1).getMutator()).setValueCount(ctr + 1);
                        break;
                    case LONG:
                        if (row.getCell(i).isMissing()) {
                            if (serializationOptions.getConvertMissingToPython()) {
                                val_length[i + 1]++;
                                ((NullableBigIntVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr,
                                        serializationOptions.getSentinelForType(Type.LONG));
                            } else {
                                ((NullableBigIntVector.Mutator) vecs.get(i + 1).getMutator()).setNull(ctr);
                            }
                        } else {
                            val_length[i + 1]++;
                            ((NullableBigIntVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr,
                                    row.getCell(i).getLongValue().longValue());
                        }
                        ((NullableBigIntVector.Mutator) vecs.get(i + 1).getMutator()).setValueCount(ctr + 1);
                        break;
                    case DOUBLE:
                        if (row.getCell(i).isMissing()) {
                            ((NullableFloat8Vector.Mutator) vecs.get(i + 1).getMutator()).setNull(ctr);
                        } else {
                            val_length[i + 1]++;
                            ((NullableFloat8Vector.Mutator) vecs.get(i + 1).getMutator()).set(ctr,
                                    row.getCell(i).getDoubleValue().doubleValue());
                        }
                        ((NullableFloat8Vector.Mutator) vecs.get(i + 1).getMutator()).setValueCount(ctr + 1);
                        break;
                    case STRING:
                        if (ctr >= ((NullableVarCharVector) vecs.get(i + 1)).getValueCapacity()) {
                            ((NullableVarCharVector) vecs.get(i + 1)).reAlloc();
                        }
                        if (row.getCell(i).isMissing()) {
                            ((NullableVarCharVector.Mutator) vecs.get(i + 1).getMutator()).setNull(ctr);
                        } else {
                            byte[] bVal = row.getCell(i).getStringValue().getBytes("UTF-8");
                            val_length[i + 1] += bVal.length;
                            // TODO ugly, because:
                            // 1. every time reallocation is done the size of
                            // the
                            // offset vector is doubled as well
                            // 2. if the initial size was underestimated by a
                            // lot
                            // there will be a lot of unnecessary copying
                            // only fixable with improved acces to the underling
                            // code
                            while (val_length[i + 1] > ((NullableVarCharVector) vecs.get(i + 1)).getByteCapacity()) {
                                ((NullableVarCharVector) vecs.get(i + 1)).reAlloc();
                            }
                            ((NullableVarCharVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr, bVal);
                        }
                        ((NullableVarCharVector.Mutator) vecs.get(i + 1).getMutator()).setValueCount(ctr + 1);
                        break;
                    case BYTES:
                        if (ctr >= ((NullableVarBinaryVector) vecs.get(i + 1)).getValueCapacity()) {
                            ((NullableVarBinaryVector) vecs.get(i + 1)).reAlloc();
                        }
                        if (row.getCell(i).isMissing()) {
                            ((NullableVarBinaryVector.Mutator) vecs.get(i + 1).getMutator()).setNull(ctr);
                        } else {
                            //TODO ugly
                            byte[] bytes = ArrayUtils.toPrimitive(row.getCell(i).getBytesValue());
                            val_length[i + 1] += bytes.length;
                            while (val_length[i + 1] > ((NullableVarBinaryVector) vecs.get(i + 1)).getByteCapacity()) {
                                ((NullableVarBinaryVector) vecs.get(i + 1)).reAlloc();
                            }
                            ((NullableVarBinaryVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr, bytes);
                        }
                        ((NullableVarBinaryVector.Mutator) vecs.get(i + 1).getMutator()).setValueCount(ctr + 1);
                        break;
                    case INTEGER_LIST:
                        NullableVarBinaryVector ilsvec = (NullableVarBinaryVector) vecs.get(i+1);
                        if (ctr >= ((NullableVarBinaryVector) vecs.get(i + 1)).getValueCapacity()) {
                            ilsvec.reAlloc();
                        }
                        if (row.getCell(i).isMissing()) {
                            ilsvec.getMutator().setNull(ctr);
                        } else {
                            //TODO ugly

                            Integer[] objs = row.getCell(i).getIntegerArrayValue();
                            int[] integers = new int[objs.length];
                            for(int j=0; j<objs.length; j++) {
                                if(objs[j] == null) {
                                    integers[j] = 0;
                                } else {
                                    integers[j] = objs[j].intValue();
                                }
                            }
                            //len(values) + values + missings
                            int len = 4 * (integers.length + 1) + integers.length / 8 + (integers.length % 8 == 0 ? 0 : 1);
                            val_length[i + 1] += len;
                            while (val_length[i + 1] > ((NullableVarBinaryVector) vecs.get(i + 1)).getByteCapacity()) {
                                ilsvec.reAlloc();
                            }
                            ByteBuffer byteBuffer = ByteBuffer.allocate(len);        
                            IntBuffer intBuffer = byteBuffer.asIntBuffer();
                            //put value length
                            intBuffer.put(integers.length);
                            //put values
                            intBuffer.put(integers);
                            //TODO ugly
                            //missing values
                            byteBuffer.position((integers.length + 1) * 4);
                            byte b = 0;
                            for(int j=0; j<objs.length; j++) {
                                if(objs[j] != null) {
                                    b += (1 << (j % 8));
                                }
                                if(j % 8 == 7) {
                                    byteBuffer.put(b);
                                    b = 0;
                                }
                            }
                            if(objs.length % 8 != 0) {
                                byteBuffer.put(b);
                            }
                            //TODO ?
                            //align to 64bit 
                            /*int pos = byteBuffer.position();
                            if(pos % 8 != 0) {
                                byteBuffer.position(8 * (pos / 8 + 1));
                            }*/
                            ilsvec.getMutator().set(ctr, byteBuffer.array());
                        }
                        ((NullableVarBinaryVector.Mutator) vecs.get(i + 1).getMutator()).setValueCount(ctr + 1);
                        break;
                    case INTEGER_SET:
                        NullableVarBinaryVector isetvec = (NullableVarBinaryVector) vecs.get(i+1);
                        if (ctr >= ((NullableVarBinaryVector) vecs.get(i + 1)).getValueCapacity()) {
                            isetvec.reAlloc();
                        }
                        if (row.getCell(i).isMissing()) {
                            isetvec.getMutator().setNull(ctr);
                        } else {
                            //TODO ugly
                            Integer[] objs = row.getCell(i).getIntegerArrayValue();
                            int[] integers = new int[objs.length];
                            boolean hasMissing = false;
                            //Put missing value to last array position
                            for(int j=0; j<objs.length; j++) {
                                if(objs[j] == null) {
                                    hasMissing = true;
                                } else if(hasMissing) {
                                    integers[j - 1] = objs[j].intValue();
                                } else {
                                    integers[j] = objs[j].intValue();
                                }
                            }
                            //len(values) + values + missings
                            int len = 4 * (integers.length + 1) + 1;
                            val_length[i + 1] += len;
                            while (val_length[i + 1] > ((NullableVarBinaryVector) vecs.get(i + 1)).getByteCapacity()) {
                                isetvec.reAlloc();
                            }
                            ByteBuffer byteBuffer = ByteBuffer.allocate(len);        
                            IntBuffer intBuffer = byteBuffer.asIntBuffer();
                            //put value length
                            intBuffer.put(integers.length);
                            //put values
                            intBuffer.put(integers);
                            //TODO ugly
                            //missing values
                            byteBuffer.position((integers.length + 1) * 4);
                            if(hasMissing) {
                                byteBuffer.put((byte)0);
                            } else {
                                byteBuffer.put((byte)1);
                            }
                            //TODO ?
                            //align to 64bit 
                            /*int pos = byteBuffer.position();
                            if(pos % 8 != 0) {
                                byteBuffer.position(8 * (pos / 8 + 1));
                            }*/
                            isetvec.getMutator().set(ctr, byteBuffer.array());
                        }
                        ((NullableVarBinaryVector.Mutator) vecs.get(i + 1).getMutator()).setValueCount(ctr + 1);
                        break;
                    default:
                        throw new IllegalStateException(
                                "Serialization is not implemented for type: " + spec.getColumnTypes()[i].name());
                    }
                }
                ctr++;
            }

            // Does not work -> why ?
            /*
             * for (int i = 0; i < spec.getNumberColumns() + 1; i++) {
             * ((ValueVector.Mutator)
             * vecs.get(i).getMutator()).setValueCount(ctr + 1); }
             */

            Map<String, String> metadata = new HashMap<String, String>();
            metadata.put("pandas", metadataBuilder.build().toString());
            Schema schema = new Schema(fields, metadata);
            VectorSchemaRoot vsr = new VectorSchemaRoot(schema, vecs, ctr);

            ArrowStreamWriter writer = new ArrowStreamWriter(vsr, null, fc);
            writer.writeBatch();
            writer.close();
            fc.close();

            return path.getBytes("UTF-8");
    }
    
    private byte[] tableToBytesFixedSize(TableIterator tableIterator, SerializationOptions serializationOptions,
            FileChannel fc, String path) throws IOException {
     // Get metadata
        final String INDEX_COL_NAME = "__index_level_0__";
        JsonObjectBuilder metadataBuilder = Json.createObjectBuilder();
        TableSpec spec = tableIterator.getTableSpec();
        JsonArrayBuilder icBuilder = Json.createArrayBuilder();
        icBuilder.add(INDEX_COL_NAME);
        metadataBuilder.add("index_columns", icBuilder);
        JsonArrayBuilder colBuilder = Json.createArrayBuilder();
        
        
        int perRowEstimateBit = 0;
        // Row ids
        JsonObjectBuilder rowIdBuilder = createColumnMetadataBuilder(INDEX_COL_NAME, "unicode", "object",
                Type.STRING, "");
        //row_id length + offsetVector entry length + null vector entry length
        perRowEstimateBit += 8 * (ASSUMED_ROWID_VAL_BYTE_SIZE + 4) + 1;
        colBuilder.add(rowIdBuilder);
        //Regular columns
        for (int i = 0; i < spec.getNumberColumns(); i++) {
            JsonObjectBuilder colMetadataBuilder;
            switch (spec.getColumnTypes()[i]) {
            case BOOLEAN:
                colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "bool", "object",
                        Type.BOOLEAN, "");
                //Null vector + value vector = 2 Bit
                perRowEstimateBit += 2;
                break;
            case INTEGER:
                colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "int", "int32",
                        Type.INTEGER, "");
                perRowEstimateBit += 33;
                break;
            case LONG:
                colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "int", "int64",
                        Type.LONG, "");
                perRowEstimateBit += 65;
                break;    
            case DOUBLE:
                colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "int", "float64",
                        Type.DOUBLE, "");
                perRowEstimateBit += 65;
                break;    
            case STRING:
                colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "unicode", "object",
                        Type.STRING, "");
                //string length + offsetVector entry length + null vector entry length
                perRowEstimateBit += 8 * (ASSUMED_STRING_VAL_BYTE_SIZE + 4) + 1;
                break;
            case BYTES:
                colMetadataBuilder = createColumnMetadataBuilder(spec.getColumnNames()[i], "bytes", "object",
                        Type.BYTES, spec.getColumnSerializers().get(spec.getColumnNames()[i]));
                //string length + offsetVector entry length + null vector entry length
                perRowEstimateBit += 8 * (ASSUMED_BYTES_VAL_BYTE_SIZE + 4) + 1;
                break;
            default:
                throw new IllegalStateException(
                        "Serialization is not implemented for type: " + spec.getColumnTypes()[i].name());
            }
            colBuilder.add(colMetadataBuilder);
        }
        metadataBuilder.add("columns", colBuilder);
        //Build pandas metadata
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("pandas", metadataBuilder.build().toString());
        
        //Maximum overhead per column 2 * 63 Bit < 16 Byte
        int numRowsEstimateBatch = (int) (8 * (FIXED_BATCH_BYTE_SIZE - 16) / perRowEstimateBit);
        
        Row cachRow = null;
        ArrowBatchWriter writer = null;
        Schema schema = null;

        while(tableIterator.hasNext()) {
            
            BufferAllocator allocator = new RootAllocator(FIXED_BATCH_BYTE_SIZE);
            
            /*int numRowsInBatch = Math.min(numRowsEstimateBatch, 
                    tableIterator.getNumberRemainingRows() + ((cachRow == null) ? 0 : 1)); */
            int numRowsInBatch = nextSmallerPowerOfTwo(numRowsEstimateBatch);
            
            List<FieldVector> vecs = new ArrayList<FieldVector>();
            List<Field> fields = new ArrayList<Field>();
        
            //Row ids
            NullableVarCharVector rowIdVector = new NullableVarCharVector(INDEX_COL_NAME, allocator);
            rowIdVector.allocateNew(nextSmallerPowerOfTwo(ASSUMED_ROWID_VAL_BYTE_SIZE * numRowsInBatch),
                    numRowsInBatch - 1);
            vecs.add(rowIdVector);
            fields.add(rowIdVector.getField());
            // Create FieldVectors and metadata
            for (int i = 0; i < spec.getNumberColumns(); i++) {
                
                FieldVector vec;
                switch (spec.getColumnTypes()[i]) {
                case BOOLEAN:
                    
                    // Allocate vector for column
                    NullableBitVector bovec = new NullableBitVector(spec.getColumnNames()[i], allocator);
                    bovec.allocateNew(numRowsInBatch);
                    vec = bovec;
                    break;
                case INTEGER:
                    
                    // Allocate vector for column
                    NullableIntVector ivec = new NullableIntVector(spec.getColumnNames()[i], allocator);
                    ivec.allocateNew(numRowsInBatch);
                    vec = ivec;
                    break;
                case LONG:
                   
                    // Allocate vector for column
                    NullableBigIntVector lvec = new NullableBigIntVector(spec.getColumnNames()[i], allocator);
                    lvec.allocateNew(numRowsInBatch);
                    vec = lvec;
                    break;    
                case DOUBLE:
                   
                    // Allocate vector for column
                    NullableFloat8Vector dvec = new NullableFloat8Vector(spec.getColumnNames()[i], allocator);
                    dvec.allocateNew(numRowsInBatch);
                    vec = dvec;
                    break;    
                case STRING:
                    
                    NullableVarCharVector vvec = new NullableVarCharVector(spec.getColumnNames()[i], allocator);
                    vvec.allocateNew(nextSmallerPowerOfTwo(ASSUMED_STRING_VAL_BYTE_SIZE * numRowsInBatch),
                            numRowsInBatch - 1);
                    vec = vvec;
                    break;
                case BYTES:
                    
                    NullableVarBinaryVector bvec = new NullableVarBinaryVector(spec.getColumnNames()[i], allocator);
                    bvec.allocateNew(nextSmallerPowerOfTwo(ASSUMED_BYTES_VAL_BYTE_SIZE * numRowsInBatch),
                            numRowsInBatch - 1);
                    vec = bvec;
                    break;
                default:
                    throw new IllegalStateException(
                            "Serialization is not implemented for type: " + spec.getColumnTypes()[i].name());
                }
                vecs.add(vec);
                fields.add(vec.getField());
            }

            int ctr = 0;
            int[] val_length = new int[vecs.size()];
            boolean bufferFull = false;
            boolean first = true;

            //TODO check row size < FIXED_BUFFER_SIZE 
            while (tableIterator.hasNext() && !bufferFull && ctr < numRowsInBatch) {
                Row row;
                if(first && cachRow != null) {
                    row = cachRow;
                    cachRow = null;
                } else {
                    row = tableIterator.next();
                }
                first = false;
                byte[] bRowKey = row.getRowKey().getBytes("UTF-8");
                val_length[0] += bRowKey.length;
                if (val_length[0] > ((NullableVarCharVector) vecs.get(0)).getByteCapacity()) {
                    bufferFull = true;
                } else {
                    ((NullableVarCharVector.Mutator) vecs.get(0).getMutator()).set(ctr, bRowKey);
                    for (int i = 0; i < spec.getNumberColumns(); i++) {
                        if(bufferFull) {
                            break;
                        }
                        switch (spec.getColumnTypes()[i]) {
                        case BOOLEAN:
                            if (row.getCell(i).isMissing()) {
                                ((NullableBitVector.Mutator) vecs.get(i + 1).getMutator()).setNull(ctr);
                            } else {
                                val_length[i + 1]++;
                                ((NullableBitVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr,
                                        row.getCell(i).getBooleanValue().booleanValue() ? 1: 0);
                            }
                            break; 
                        case INTEGER:
                            if (row.getCell(i).isMissing()) {
                                if(serializationOptions.getConvertMissingToPython()) {
                                    val_length[i + 1]++;
                                    ((NullableIntVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr,
                                            (int) serializationOptions.getSentinelForType(Type.INTEGER));
                                } else {
                                    ((NullableIntVector.Mutator) vecs.get(i + 1).getMutator()).setNull(ctr);
                                }
                            } else {
                                val_length[i + 1]++;
                                ((NullableIntVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr,
                                        row.getCell(i).getIntegerValue().intValue());
                            }
                            break;
                        case LONG:
                            if (row.getCell(i).isMissing()) {
                                if(serializationOptions.getConvertMissingToPython()) {
                                    val_length[i + 1]++;
                                    ((NullableBigIntVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr,
                                            serializationOptions.getSentinelForType(Type.LONG));
                                } else {
                                    ((NullableBigIntVector.Mutator) vecs.get(i + 1).getMutator()).setNull(ctr);
                                }
                            } else {
                                val_length[i + 1]++;
                                ((NullableBigIntVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr,
                                        row.getCell(i).getLongValue().longValue());
                            }
                            break;
                        case DOUBLE:
                            if (row.getCell(i).isMissing()) {
                                ((NullableFloat8Vector.Mutator) vecs.get(i + 1).getMutator()).setNull(ctr);
                            } else {
                                val_length[i + 1]++;
                                ((NullableFloat8Vector.Mutator) vecs.get(i + 1).getMutator()).set(ctr,
                                        row.getCell(i).getDoubleValue().doubleValue());
                            }
                            break;
                        case STRING:
                            if(ctr >= ((NullableVarCharVector) vecs.get(i + 1)).getValueCapacity()) {
                                bufferFull = true;
                                break;
                            }
                            if (row.getCell(i).isMissing()) {
                                ((NullableVarCharVector.Mutator) vecs.get(i + 1).getMutator()).setNull(ctr);
                            } else {
                                byte[] bVal = row.getCell(i).getStringValue().getBytes("UTF-8");
                                val_length[i + 1] += bVal.length;
                                if (val_length[i + 1] > ((NullableVarCharVector) vecs.get(i + 1)).getByteCapacity()) {
                                    bufferFull = true;
                                    break;
                                }
                                ((NullableVarCharVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr, bVal);
                            }
                            break;
                        case BYTES:
                            if(ctr >= ((NullableVarBinaryVector) vecs.get(i + 1)).getValueCapacity()) {
                                bufferFull = true;
                                break;
                            }
                            if (row.getCell(i).isMissing()) {
                                ((NullableVarBinaryVector.Mutator) vecs.get(i + 1).getMutator()).setNull(ctr);
                            } else {
                                //TODO ugly
                                byte[] bytes = ArrayUtils.toPrimitive(row.getCell(i).getBytesValue());
                                val_length[i + 1] += bytes.length;
                                if (val_length[i + 1] > ((NullableVarBinaryVector) vecs.get(i + 1)).getByteCapacity()) {
                                    bufferFull = true;
                                    break;
                                }
                                ((NullableVarBinaryVector.Mutator) vecs.get(i + 1).getMutator()).set(ctr, bytes);
                            }
                            break;
                        default:
                            throw new IllegalStateException(
                                    "Serialization is not implemented for type: " + spec.getColumnTypes()[i].name());
                        }
                    }
                    if(!bufferFull) {
                        for (int col = 0; col < spec.getNumberColumns() + 1; col++) {
                            ((ValueVector.Mutator) vecs.get(col).getMutator()).setValueCount(ctr + 1);
                        }
                    }
                }
                if(bufferFull) {
                    cachRow = row;
                } else {
                    ctr++;
                }
            }
            
            VectorSchemaRoot vsr;
            if(writer == null) {
                schema = new Schema(fields, metadata);
                vsr = new VectorSchemaRoot(schema, vecs, ctr);
                writer = new ArrowBatchWriter(vsr, null, fc);
            } else {
                vsr = new VectorSchemaRoot(schema, vecs, ctr);
            }
            writer.writeRecordBatch(new VectorUnloader(vsr).getRecordBatch());
        }
        writer.close();
        fc.close();

        return path.getBytes("UTF-8");
    }
    
    ArrowStreamReader m_streamReader = null;
    
    private ArrowStreamReader getReader(String path) throws FileNotFoundException {
        if(m_streamReader == null) {
                FileChannel fc = new RandomAccessFile(new File(path), "rw").getChannel();
                ArrowStreamReader reader = new ArrowStreamReader((ReadableByteChannel) fc, 
                        (BufferAllocator) new RootAllocator(Long.MAX_VALUE));
                m_streamReader = reader;
        }
        return m_streamReader;
    }

    @Override
    public void bytesIntoTable(TableCreator<?> tableCreator, byte[] bytes, SerializationOptions serializationOptions) {
        //TODO sentinel
        String path = new String(bytes);
        TableSpec spec = tableSpecFromBytes(bytes);
        try {
            ArrowStreamReader reader = getReader(path);
            //Index is always string
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            NullableVarCharVector indexCol = (NullableVarCharVector) root.getVector(m_indexColumnName);
            NullableVarCharVector.Accessor rowKeyAccessor = (NullableVarCharVector.Accessor) indexCol.getAccessor();
            Accessor[] accessors = new Accessor[spec.getNumberColumns()];
            
            for(int j=0; j<spec.getNumberColumns(); j++) {
                accessors[j] = root.getVector(spec.getColumnNames()[j]).getAccessor();
            }
            
            for(int i=0; i<rowKeyAccessor.getValueCount(); i++) {
               Row row = new RowImpl(rowKeyAccessor.getObject(i).toString(), spec.getNumberColumns());
               for(int j=0; j<spec.getNumberColumns(); j++) {
                   if ( accessors[j].isNull(i) ) {
                       row.setCell(new CellImpl(), j);
                   } else {
                       if(spec.getColumnTypes()[j] == Type.STRING) {
                           row.setCell( new CellImpl(((NullableVarCharVector.Accessor) root.getVector(spec.getColumnNames()[j]).getAccessor()).getObject(i).toString()), j );
                       } else if(spec.getColumnTypes()[j] == Type.LONG) {
                           long lval = ((NullableBigIntVector.Accessor) root.getVector(spec.getColumnNames()[j]).getAccessor()).get(i);
                           if(serializationOptions.getConvertMissingFromPython() && serializationOptions.isSentinel(Type.LONG, lval)) {
                               row.setCell( new CellImpl(), j );
                           } else {
                               row.setCell( new CellImpl(lval), j );
                           }
                       } else if(spec.getColumnTypes()[j] == Type.INTEGER) {
                           int ival = ((NullableIntVector.Accessor) root.getVector(spec.getColumnNames()[j]).getAccessor()).get(i);
                           if(serializationOptions.getConvertMissingFromPython() && serializationOptions.isSentinel(Type.INTEGER, ival)) {
                               row.setCell( new CellImpl(), j );
                           } else {
                               row.setCell( new CellImpl(ival), j );
                           }
                       } else if(spec.getColumnTypes()[j] == Type.DOUBLE) {
                           row.setCell( new CellImpl(((NullableFloat8Vector.Accessor) accessors[j]).get(i)), j );
                       } else if(spec.getColumnTypes()[j] == Type.BOOLEAN) {
                           row.setCell( new CellImpl(((NullableBitVector.Accessor) accessors[j]).get(i) > 0), j );
                       } else if(spec.getColumnTypes()[j] == Type.BYTES) {
                           //TODO ugly
                           row.setCell( new CellImpl( ArrayUtils.toObject(((NullableVarBinaryVector.Accessor) root.getVector(spec.getColumnNames()[j]).getAccessor()).getObject(i)) ), j );
                       } else if(spec.getColumnTypes()[j] == Type.INTEGER_LIST || spec.getColumnTypes()[j] == Type.INTEGER_SET) {
                           ByteBuffer buffer = ByteBuffer.wrap(((NullableVarBinaryVector.Accessor) root.getVector(spec.getColumnNames()[j]).getAccessor()).getObject(i)).order(ByteOrder.LITTLE_ENDIAN);
                           IntBuffer ibuffer = buffer.asIntBuffer();
                           int nVals = ibuffer.get();
                           int[] iar = new int[nVals];
                           ibuffer.get(iar);
                           //TODO ugly
                           ArrayList<Integer> ls = new ArrayList<>(Arrays.asList(ArrayUtils.toObject(iar)));
                           buffer.position((nVals + 1)*4);
                           boolean isset = (spec.getColumnTypes()[j] == Type.INTEGER_SET);
                           if(!isset) {
                           byte b = 0;
                               for(int idx=0; idx<nVals; idx++) {
                                   if(idx % 8 == 0) {
                                       b = buffer.get();
                                   }
                                   if((b & (1 << (idx %8))) == 0) {
                                       ls.set(idx, null);
                                   }
                               }
                           } else {
                               if(buffer.get() == 0) {
                                   ls.add(null);
                               }
                           }
                           row.setCell( new CellImpl( ls.toArray(new Integer[ls.size()]), isset  ), j );
                       } else {
                           throw new IllegalStateException("Unknown column type!");
                       }
                   }
               }
               tableCreator.addRow(row);
            }
            reader.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        m_streamReader = null;
        m_tableSpec = null;
    }
    
    TableSpec m_tableSpec = null;
    //NOTE: we will never get a multiindex due to index standartization in FromPandasTable
    String m_indexColumnName = null;

    @Override
    public TableSpec tableSpecFromBytes(byte[] bytes) {
        
        if(m_tableSpec == null) {
            String path = new String(bytes);
            try {
                ArrowStreamReader reader = getReader(path);
                boolean succ = reader.loadNextBatch();
                //NodeLogger.getLogger("test").warn("Success on loading batch: " + succ);
                Schema schema = reader.getVectorSchemaRoot().getSchema();
                Map<String,String> metadata = schema.getCustomMetadata();
                Map<String,String> columnSerializers = new HashMap<String,String>();
                String pandas_metadata = metadata.get("pandas");
                if(pandas_metadata != null) {
                    //NodeLogger.getLogger("test").warn("pandas metadata found: " + pandas_metadata);
                    JsonReader jsreader = Json.createReader(new StringReader(pandas_metadata));
                    JsonObject jpandas_metadata = jsreader.readObject();
                    JsonArray index_cols = jpandas_metadata.getJsonArray("index_columns");
                    JsonArray cols = jpandas_metadata.getJsonArray("columns");
                    String[] names = new String[cols.size() - index_cols.size()];
                    Type[] types = new Type[cols.size() - index_cols.size()];
                    int noIdxCtr = 0;
                    for(int i=0; i<cols.size(); i++) {
                        JsonObject col = cols.getJsonObject(i);
                        boolean contained = false;
                        for(int j=0; j<index_cols.size(); j++) {
                            if(index_cols.getString(j).contentEquals(col.getString("name"))) {
                                contained = true;
                                break;
                            }
                        }
                        if(contained) {
                            m_indexColumnName = col.getString("name");
                            continue;
                        }
                        names[noIdxCtr] = col.getString("name");
                        JsonObject typeObj = col.getJsonObject("metadata");
                        types[noIdxCtr] = Type.getTypeForId(typeObj.getInt("type_id"));
                        if(types[noIdxCtr] == Type.BYTES) {
                            columnSerializers.put(names[noIdxCtr], col.getJsonObject("metadata").getString("serializer_id")); 
                        }
                        noIdxCtr++;
                    }
                    m_tableSpec = new TableSpecImpl(types, names, columnSerializers);
                }
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        if(m_tableSpec == null) {
            throw new IllegalStateException("Could not build TableSpec!");
        }
        
        return m_tableSpec;
    }
}