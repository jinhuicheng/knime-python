/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   Sep 25, 2014 (Patrick Winter): created
 */
package org.knime.python.kernel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.MissingCell;
import org.knime.core.data.StringValue;
import org.knime.core.data.collection.CollectionCellFactory;
import org.knime.core.data.collection.CollectionDataValue;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.collection.SetCell;
import org.knime.core.data.collection.SetDataValue;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.date.DateAndTimeCell;
import org.knime.core.data.date.DateAndTimeValue;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.BooleanCell.BooleanCellFactory;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.filestore.FileStoreFactory;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.python.kernel.proto.ProtobufKnimeTable;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.BooleanColumn;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.BooleanListColumn;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.ColumnReference;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.DateAndTimeColumn;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.DateAndTimeListColumn;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.DoubleColumn;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.DoubleListColumn;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.IntegerColumn;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.IntegerListColumn;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.LongColumn;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.LongListColumn;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.ObjectColumn;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.ObjectListColumn;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.StringColumn;
import org.knime.python.kernel.proto.ProtobufKnimeTable.Table.StringListColumn;
import org.knime.python.typeextension.Deserializer;
import org.knime.python.typeextension.DeserializerFactory;
import org.knime.python.typeextension.KnimeToPythonExtension;
import org.knime.python.typeextension.KnimeToPythonExtensions;
import org.knime.python.typeextension.PythonToKnimeExtensions;
import org.knime.python.typeextension.Serializer;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

/**
 * Provides methods to convert from a {@link ProtobufKnimeTable} to a
 * {@link BufferedDataTable} and visa verse.
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 */
class ProtobufConverter {

    /**
     * Creates a protobuf table message representing the given table.
     *
     * @param table
     *            The table that the protobuf message will be based on
     * @param chunkSize
     *            How big a single chunk will be
     * @param rowIterator
     *            Iterator to get the rows
     * @param chunk
     *            Number of the current chunk
     * @param executionMonitor
     *            Monitor that will be updated about progress
     * @param rowLimit
     *            The maximum number of rows to transfer
     * @return The protobuf table message
     * @throws IOException
     *             If an error occured
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static Table dataTableToProtobuf(final BufferedDataTable table, final int chunkSize,
        final CloseableRowIterator rowIterator, final int chunk, final ExecutionMonitor executionMonitor,
        final int rowLimit, final KnimeToPythonExtensions knimeToPythonExtensions) throws IOException {
        final Table.Builder tableBuilder = ProtobufKnimeTable.Table.newBuilder();
        tableBuilder.setValid(true);
        tableBuilder.setNumCols(table.getDataTableSpec().getNumColumns());
        final List<GeneratedMessage.Builder> columnBuilders = new ArrayList<GeneratedMessage.Builder>();
        final Map<String, Integer> nextTypeIndexMap = new HashMap<String, Integer>();
        int index = 0;
        for (final DataColumnSpec colSpec : table.getDataTableSpec()) {
            String type;
            if (colSpec.getType().isCompatible(BooleanValue.class)) {
                type = "BooleanColumn";
                columnBuilders.add(BooleanColumn.newBuilder().setName(colSpec.getName()));
            } else if (colSpec.getType().isCompatible(IntValue.class)) {
                type = "IntegerColumn";
                columnBuilders.add(IntegerColumn.newBuilder().setName(colSpec.getName()));
            } else if (colSpec.getType().isCompatible(LongValue.class)) {
                type = "LongColumn";
                columnBuilders.add(LongColumn.newBuilder().setName(colSpec.getName()));
            } else if (colSpec.getType().isCompatible(DoubleValue.class)) {
                type = "DoubleColumn";
                columnBuilders.add(DoubleColumn.newBuilder().setName(colSpec.getName()));
            } else if (colSpec.getType().isCompatible(DateAndTimeValue.class)) {
                type = "DateAndTimeColumn";
                columnBuilders.add(DateAndTimeColumn.newBuilder().setName(colSpec.getName()));
            } else if (colSpec.getType().isCollectionType()
                    && colSpec.getType().getCollectionElementType().isCompatible(BooleanValue.class)) {
                type = "BooleanListColumn";
                columnBuilders.add(BooleanListColumn.newBuilder().setName(colSpec.getName())
                    .setIsSet(colSpec.getType().isCompatible(SetDataValue.class)));
            } else if (colSpec.getType().isCollectionType()
                    && colSpec.getType().getCollectionElementType().isCompatible(IntValue.class)) {
                type = "IntegerListColumn";
                columnBuilders.add(IntegerListColumn.newBuilder().setName(colSpec.getName())
                    .setIsSet(colSpec.getType().isCompatible(SetDataValue.class)));
            } else if (colSpec.getType().isCollectionType()
                    && colSpec.getType().getCollectionElementType().isCompatible(LongValue.class)) {
                type = "LongListColumn";
                columnBuilders.add(LongListColumn.newBuilder().setName(colSpec.getName())
                    .setIsSet(colSpec.getType().isCompatible(SetDataValue.class)));
            } else if (colSpec.getType().isCollectionType()
                    && colSpec.getType().getCollectionElementType().isCompatible(DoubleValue.class)) {
                type = "DoubleListColumn";
                columnBuilders.add(DoubleListColumn.newBuilder().setName(colSpec.getName())
                    .setIsSet(colSpec.getType().isCompatible(SetDataValue.class)));
            } else if (colSpec.getType().isCollectionType()
                    && colSpec.getType().getCollectionElementType().isCompatible(DateAndTimeValue.class)) {
                type = "DateAndTimeListColumn";
                columnBuilders.add(DateAndTimeListColumn.newBuilder().setName(colSpec.getName())
                    .setIsSet(colSpec.getType().isCompatible(SetDataValue.class)));
            } else if (colSpec.getType().isCollectionType()) {
                final KnimeToPythonExtension typeExtension = KnimeToPythonExtensions.getExtension(colSpec.getType()
                    .getCollectionElementType());
                if (typeExtension != null) {
                    type = "ObjectListColumn";
                    columnBuilders.add(ObjectListColumn.newBuilder().setName(colSpec.getName())
                        .setType(typeExtension.getId())
                        .setIsSet(colSpec.getType().isCompatible(SetDataValue.class)));
                } else {
                    type = "StringListColumn";
                    columnBuilders.add(StringListColumn.newBuilder().setName(colSpec.getName())
                        .setIsSet(colSpec.getType().isCompatible(SetDataValue.class)));
                }
            } else {
                final KnimeToPythonExtension typeExtension = KnimeToPythonExtensions.getExtension(colSpec.getType());
                if (typeExtension != null) {
                    type = "ObjectColumn";
                    columnBuilders.add(ObjectColumn.newBuilder().setName(colSpec.getName())
                        .setType(typeExtension.getId()));
                } else {
                    type = "StringColumn";
                    columnBuilders.add(StringColumn.newBuilder().setName(colSpec.getName()));
                }
            }
            final int typeIndex = nextTypeIndexMap.containsKey(type) ? nextTypeIndexMap.get(type) : 0;
            nextTypeIndexMap.put(type, typeIndex + 1);
            final ColumnReference.Builder colRefBuilder = ColumnReference.newBuilder().setType(type)
                    .setIndexInType(typeIndex);
            tableBuilder.addColRef(colRefBuilder.build());
            index++;
        }
        int numRows = 0;
        while (rowIterator.hasNext() && (numRows < chunkSize) && (((chunk * chunkSize) + numRows) < rowLimit)) {
            final DataRow row = rowIterator.next();
            tableBuilder.addRowId(row.getKey().getString());
            index = 0;
            for (int i = 0; i < row.getNumCells(); i++) {
                final DataCell cell = row.getCell(i);
                final GeneratedMessage.Builder builder = columnBuilders.get(index);
                if (builder instanceof BooleanColumn.Builder) {
                    final BooleanColumn.Builder boolColumn = (BooleanColumn.Builder) builder;
                    final Table.BooleanValue.Builder booleanValueBuilder = Table.BooleanValue.newBuilder();
                    if (!cell.isMissing()) {
                        booleanValueBuilder.setValue(((BooleanValue) cell).getBooleanValue());
                    }
                    boolColumn.addBooleanValue(booleanValueBuilder.build());
                } else if (builder instanceof IntegerColumn.Builder) {
                    final IntegerColumn.Builder intColumn = (IntegerColumn.Builder) builder;
                    final Table.IntegerValue.Builder integerValueBuilder = Table.IntegerValue.newBuilder();
                    if (!cell.isMissing()) {
                        integerValueBuilder.setValue(((IntValue) cell).getIntValue());
                    }
                    intColumn.addIntegerValue(integerValueBuilder.build());
                } else if (builder instanceof LongColumn.Builder) {
                    final LongColumn.Builder longColumn = (LongColumn.Builder) builder;
                    final Table.LongValue.Builder longValueBuilder = Table.LongValue.newBuilder();
                    if (!cell.isMissing()) {
                        longValueBuilder.setValue(((LongValue) cell).getLongValue());
                    }
                    longColumn.addLongValue(longValueBuilder.build());
                } else if (builder instanceof DoubleColumn.Builder) {
                    final DoubleColumn.Builder doubleColumn = (DoubleColumn.Builder) builder;
                    final Table.DoubleValue.Builder doubleValueBuilder = Table.DoubleValue.newBuilder();
                    if (!cell.isMissing()) {
                        doubleValueBuilder.setValue(((DoubleValue) cell).getDoubleValue());
                    }
                    doubleColumn.addDoubleValue(doubleValueBuilder.build());
                } else if (builder instanceof StringColumn.Builder) {
                    final StringColumn.Builder stringColumn = (StringColumn.Builder) builder;
                    final Table.StringValue.Builder stringValueBuilder = Table.StringValue.newBuilder();
                    if (!cell.isMissing()) {
                        if (cell.getType().isCompatible(StringValue.class)) {
                            stringValueBuilder.setValue(((StringValue) cell).getStringValue());
                        } else {
                            stringValueBuilder.setValue(cell.toString());
                        }
                    }
                    stringColumn.addStringValue(stringValueBuilder.build());
                } else if (builder instanceof DateAndTimeColumn.Builder) {
                    final DateAndTimeColumn.Builder dateAndTimeColumn = (DateAndTimeColumn.Builder) builder;
                    dateAndTimeColumn.addDateAndTimeValue(dateValueFromCell(cell));
                } else if (builder instanceof BooleanListColumn.Builder) {
                    final BooleanListColumn.Builder booleanListColumn = (BooleanListColumn.Builder) builder;
                    final Table.BooleanListValue.Builder booleanListValueBuilder = Table.BooleanListValue.newBuilder();
                    booleanListValueBuilder.setIsMissing(cell.isMissing());
                    if (!cell.isMissing()) {
                        final CollectionDataValue collectionCell = (CollectionDataValue) cell;
                        for (final DataCell singleCell : collectionCell) {
                            final Table.BooleanValue.Builder booleanValue = Table.BooleanValue.newBuilder();
                            if (!singleCell.isMissing()) {
                                booleanValue.setValue(((BooleanValue) singleCell).getBooleanValue());
                            }
                            booleanListValueBuilder.addValue(booleanValue);
                        }
                    }
                    booleanListColumn.addBooleanListValue(booleanListValueBuilder.build());
                } else if (builder instanceof IntegerListColumn.Builder) {
                    final IntegerListColumn.Builder integerListColumn = (IntegerListColumn.Builder) builder;
                    final Table.IntegerListValue.Builder integerListValueBuilder = Table.IntegerListValue.newBuilder();
                    integerListValueBuilder.setIsMissing(cell.isMissing());
                    if (!cell.isMissing()) {
                        final CollectionDataValue collectionCell = (CollectionDataValue) cell;
                        for (final DataCell singleCell : collectionCell) {
                            final Table.IntegerValue.Builder integerValue = Table.IntegerValue.newBuilder();
                            if (!singleCell.isMissing()) {
                                integerValue.setValue(((IntValue) singleCell).getIntValue());
                            }
                            integerListValueBuilder.addValue(integerValue);
                        }
                    }
                    integerListColumn.addIntegerListValue(integerListValueBuilder.build());
                } else if (builder instanceof LongListColumn.Builder) {
                    final LongListColumn.Builder longListColumn = (LongListColumn.Builder) builder;
                    final Table.LongListValue.Builder longListValueBuilder = Table.LongListValue.newBuilder();
                    longListValueBuilder.setIsMissing(cell.isMissing());
                    if (!cell.isMissing()) {
                        final CollectionDataValue collectionCell = (CollectionDataValue) cell;
                        for (final DataCell singleCell : collectionCell) {
                            final Table.LongValue.Builder longValue = Table.LongValue.newBuilder();
                            if (!singleCell.isMissing()) {
                                longValue.setValue(((LongValue) singleCell).getLongValue());
                            }
                            longListValueBuilder.addValue(longValue);
                        }
                    }
                    longListColumn.addLongListValue(longListValueBuilder.build());
                } else if (builder instanceof DoubleListColumn.Builder) {
                    final DoubleListColumn.Builder doubleListColumn = (DoubleListColumn.Builder) builder;
                    final Table.DoubleListValue.Builder doubleListValueBuilder = Table.DoubleListValue.newBuilder();
                    doubleListValueBuilder.setIsMissing(cell.isMissing());
                    if (!cell.isMissing()) {
                        final CollectionDataValue collectionCell = (CollectionDataValue) cell;
                        for (final DataCell singleCell : collectionCell) {
                            final Table.DoubleValue.Builder doubleValue = Table.DoubleValue.newBuilder();
                            if (!singleCell.isMissing()) {
                                doubleValue.setValue(((DoubleValue) singleCell).getDoubleValue());
                            }
                            doubleListValueBuilder.addValue(doubleValue);
                        }
                    }
                    doubleListColumn.addDoubleListValue(doubleListValueBuilder.build());
                } else if (builder instanceof DateAndTimeListColumn.Builder) {
                    final DateAndTimeListColumn.Builder dateAndTimeListColumn = (DateAndTimeListColumn.Builder) builder;
                    final Table.DateAndTimeListValue.Builder dateAndTimeListValueBuilder = Table.DateAndTimeListValue
                            .newBuilder();
                    dateAndTimeListValueBuilder.setIsMissing(cell.isMissing());
                    if (!cell.isMissing()) {
                        final CollectionDataValue collectionCell = (CollectionDataValue) cell;
                        for (final DataCell singleCell : collectionCell) {
                            dateAndTimeListValueBuilder.addValue(dateValueFromCell(singleCell));
                        }
                    }
                    dateAndTimeListColumn.addDateAndTimeListValue(dateAndTimeListValueBuilder.build());
                } else if (builder instanceof StringListColumn.Builder) {
                    final StringListColumn.Builder stringListColumn = (StringListColumn.Builder) builder;
                    final Table.StringListValue.Builder stringListValueBuilder = Table.StringListValue.newBuilder();
                    stringListValueBuilder.setIsMissing(cell.isMissing());
                    if (!cell.isMissing()) {
                        final CollectionDataValue collectionCell = (CollectionDataValue) cell;
                        for (final DataCell singleCell : collectionCell) {
                            final Table.StringValue.Builder stringValue = Table.StringValue.newBuilder();
                            if (!singleCell.isMissing()) {
                                if (singleCell.getType().isCompatible(StringValue.class)) {
                                    stringValue.setValue(((StringValue) singleCell).getStringValue());
                                } else {
                                    stringValue.setValue(singleCell.toString());
                                }
                            }
                            stringListValueBuilder.addValue(stringValue);
                        }
                    }
                    stringListColumn.addStringListValue(stringListValueBuilder.build());
                } else if (builder instanceof ObjectListColumn.Builder) {
                    final ObjectListColumn.Builder objectListColumn = (ObjectListColumn.Builder) builder;
                    final Table.ObjectListValue.Builder objectListValueBuilder = Table.ObjectListValue.newBuilder();
                    objectListValueBuilder.setIsMissing(cell.isMissing());
                    if (!cell.isMissing()) {
                        final CollectionDataValue collectionCell = (CollectionDataValue) cell;
                        for (final DataCell singleCell : collectionCell) {
                            final Table.ObjectValue.Builder objectValue = Table.ObjectValue.newBuilder();
                            final Serializer serializer = knimeToPythonExtensions.getSerializer(KnimeToPythonExtensions.getExtension(table.getDataTableSpec().getColumnSpec(i).getType().getCollectionElementType()).getId());
                            if (!singleCell.isMissing()) {
                                objectValue.setValue(ByteString.copyFrom(serializer.serialize(singleCell)));
                            }
                            objectListValueBuilder.addValue(objectValue);
                        }
                    }
                    objectListColumn.addObjectListValue(objectListValueBuilder.build());
                } else if (builder instanceof ObjectColumn.Builder) {
                    final ObjectColumn.Builder objectColumn = (ObjectColumn.Builder) builder;
                    final Serializer serializer = knimeToPythonExtensions.getSerializer(KnimeToPythonExtensions.getExtension(table.getDataTableSpec().getColumnSpec(i).getType()).getId());
                    final Table.ObjectValue.Builder objectValue = Table.ObjectValue.newBuilder();
                    if (!cell.isMissing()) {
                        objectValue.setValue(ByteString.copyFrom(serializer.serialize(cell)));
                    }
                    objectColumn.addObjectValue(objectValue);
                }
                index++;
            }
            numRows++;
            if (executionMonitor != null) {
                try {
                    executionMonitor.checkCanceled();
                } catch (final CanceledExecutionException e) {
                    throw new IOException(e.getMessage(), e);
                }
                executionMonitor.setProgress(((chunk * chunkSize) + numRows) / (double) rowLimit);
            }
        }
        tableBuilder.setNumRows(numRows);
        for (final GeneratedMessage.Builder builder : columnBuilders) {
            if (builder instanceof BooleanColumn.Builder) {
                tableBuilder.addBooleanCol((BooleanColumn.Builder) builder);
            } else if (builder instanceof IntegerColumn.Builder) {
                tableBuilder.addIntegerCol((IntegerColumn.Builder) builder);
            } else if (builder instanceof LongColumn.Builder) {
                tableBuilder.addLongCol((LongColumn.Builder) builder);
            } else if (builder instanceof DoubleColumn.Builder) {
                tableBuilder.addDoubleCol((DoubleColumn.Builder) builder);
            } else if (builder instanceof StringColumn.Builder) {
                tableBuilder.addStringCol((StringColumn.Builder) builder);
            } else if (builder instanceof DateAndTimeColumn.Builder) {
                tableBuilder.addDateAndTimeCol((DateAndTimeColumn.Builder) builder);
            } else if (builder instanceof BooleanListColumn.Builder) {
                tableBuilder.addBooleanListCol((BooleanListColumn.Builder) builder);
            } else if (builder instanceof IntegerListColumn.Builder) {
                tableBuilder.addIntegerListCol((IntegerListColumn.Builder) builder);
            } else if (builder instanceof LongListColumn.Builder) {
                tableBuilder.addLongListCol((LongListColumn.Builder) builder);
            } else if (builder instanceof DoubleListColumn.Builder) {
                tableBuilder.addDoubleListCol((DoubleListColumn.Builder) builder);
            } else if (builder instanceof DateAndTimeListColumn.Builder) {
                tableBuilder.addDateAndTimeListCol((DateAndTimeListColumn.Builder) builder);
            } else if (builder instanceof StringListColumn.Builder) {
                tableBuilder.addStringListCol((StringListColumn.Builder) builder);
            } else if (builder instanceof ObjectListColumn.Builder) {
                tableBuilder.addObjectListCol((ObjectListColumn.Builder) builder);
            } else {
                tableBuilder.addObjectCol((ObjectColumn.Builder) builder);
            }
        }
        return tableBuilder.build();
    }

    /**
     * Creates a container based on the specs of the given protobuf table
     * message.
     *
     * @param table
     *            The protobuf table message
     * @param exec
     *            Execution context used to create the container
     * @return Container based on the specs of the protobuf table message
     * @throws IOException
     *             If an error occured
     */
    static BufferedDataContainer createContainerFromProtobuf(final Table table, final ExecutionContext exec)
            throws IOException {
        if (!table.getValid()) {
            throw new IOException(table.getError());
        }
        final List<ColumnReference> colRefList = table.getColRefList();
        final DataColumnSpec[] colSpecs = new DataColumnSpec[colRefList.size()];
        for (int i = 0; i < colRefList.size(); i++) {
            final String colType = colRefList.get(i).getType();
            if (colType.equals("BooleanColumn")) {
                colSpecs[i] = new DataColumnSpecCreator(table.getBooleanCol(colRefList.get(i).getIndexInType())
                    .getName(), BooleanCell.TYPE).createSpec();
            } else if (colType.equals("IntegerColumn")) {
                colSpecs[i] = new DataColumnSpecCreator(table.getIntegerCol(colRefList.get(i).getIndexInType())
                    .getName(), IntCell.TYPE).createSpec();
            } else if (colType.equals("LongColumn")) {
                colSpecs[i] = new DataColumnSpecCreator(table.getLongCol(colRefList.get(i).getIndexInType()).getName(),
                    LongCell.TYPE).createSpec();
            } else if (colType.equals("DoubleColumn")) {
                colSpecs[i] = new DataColumnSpecCreator(table.getDoubleCol(colRefList.get(i).getIndexInType())
                    .getName(), DoubleCell.TYPE).createSpec();
            } else if (colType.equals("StringColumn")) {
                colSpecs[i] = new DataColumnSpecCreator(table.getStringCol(colRefList.get(i).getIndexInType())
                    .getName(), StringCell.TYPE).createSpec();
            } else if (colType.equals("DateAndTimeColumn")) {
                colSpecs[i] = new DataColumnSpecCreator(table.getDateAndTimeCol(colRefList.get(i).getIndexInType())
                    .getName(), DateAndTimeCell.TYPE).createSpec();
            } else if (colType.equals("BooleanListColumn")) {
                final BooleanListColumn column = table.getBooleanListCol(colRefList.get(i).getIndexInType());
                final DataType type = column.getIsSet() ? SetCell.getCollectionType(BooleanCell.TYPE) : ListCell
                    .getCollectionType(BooleanCell.TYPE);
                colSpecs[i] = new DataColumnSpecCreator(column.getName(), type).createSpec();
            } else if (colType.equals("IntegerListColumn")) {
                final IntegerListColumn column = table.getIntegerListCol(colRefList.get(i).getIndexInType());
                final DataType type = column.getIsSet() ? SetCell.getCollectionType(IntCell.TYPE) : ListCell
                    .getCollectionType(IntCell.TYPE);
                colSpecs[i] = new DataColumnSpecCreator(column.getName(), type).createSpec();
            } else if (colType.equals("LongListColumn")) {
                final LongListColumn column = table.getLongListCol(colRefList.get(i).getIndexInType());
                final DataType type = column.getIsSet() ? SetCell.getCollectionType(LongCell.TYPE) : ListCell
                    .getCollectionType(LongCell.TYPE);
                colSpecs[i] = new DataColumnSpecCreator(column.getName(), type).createSpec();
            } else if (colType.equals("DoubleListColumn")) {
                final DoubleListColumn column = table.getDoubleListCol(colRefList.get(i).getIndexInType());
                final DataType type = column.getIsSet() ? SetCell.getCollectionType(DoubleCell.TYPE) : ListCell
                    .getCollectionType(DoubleCell.TYPE);
                colSpecs[i] = new DataColumnSpecCreator(column.getName(), type).createSpec();
            } else if (colType.equals("DateAndTimeListColumn")) {
                final DateAndTimeListColumn column = table.getDateAndTimeListCol(colRefList.get(i).getIndexInType());
                final DataType type = column.getIsSet() ? SetCell.getCollectionType(DateAndTimeCell.TYPE) : ListCell
                    .getCollectionType(DateAndTimeCell.TYPE);
                colSpecs[i] = new DataColumnSpecCreator(column.getName(), type).createSpec();
            } else if (colType.equals("StringListColumn")) {
                final StringListColumn column = table.getStringListCol(colRefList.get(i).getIndexInType());
                final DataType type = column.getIsSet() ? SetCell.getCollectionType(StringCell.TYPE) : ListCell
                    .getCollectionType(StringCell.TYPE);
                colSpecs[i] = new DataColumnSpecCreator(column.getName(), type).createSpec();
            } else if (colType.equals("ObjectListColumn")) {
                final ObjectListColumn column = table.getObjectListCol(colRefList.get(i).getIndexInType());
                final DeserializerFactory deserializerFactory = PythonToKnimeExtensions.getExtension(column.getType()).getJavaDeserializerFactory();
                final DataType type = column.getIsSet() ? SetCell.getCollectionType(deserializerFactory.getDataType()) : ListCell
                    .getCollectionType(deserializerFactory.getDataType());
                colSpecs[i] = new DataColumnSpecCreator(column.getName(), type).createSpec();
            } else if (colType.equals("ObjectColumn")) {
                final ObjectColumn column = table.getObjectCol(colRefList.get(i).getIndexInType());
                final DeserializerFactory deserializerFactory = PythonToKnimeExtensions.getExtension(column.getType()).getJavaDeserializerFactory();
                colSpecs[i] = new DataColumnSpecCreator(column.getName(), deserializerFactory.getDataType()).createSpec();
            }
        }
        final BufferedDataContainer container = exec.createDataContainer(new DataTableSpec(colSpecs));
        return container;
    }

    /**
     * Adds rows contained in the protobuf table message to the given container.
     *
     * @param table
     *            The protobuf table message containing the rows
     * @param container
     *            The container to put the rows into
     * @param rowsOverall
     *            Total number of rows that will be put into the table (needed
     *            for progress calculation)
     * @param executionMonitor
     *            Monitor that will be updated about progress
     * @throws IOException
     *             If an error occured
     */
    static void addRowsFromProtobuf(final Table table, final BufferedDataContainer container, final int rowsOverall,
        final ExecutionMonitor executionMonitor, final FileStoreFactory fileStoreFactory, final PythonToKnimeExtensions pythonToKnimeExtensions) throws IOException {
        for (int i = 0; i < table.getNumRows(); i++) {
            container.addRowToTable(createRow(table, i, fileStoreFactory, pythonToKnimeExtensions));
            if (executionMonitor != null) {
                try {
                    executionMonitor.checkCanceled();
                } catch (final CanceledExecutionException e) {
                    throw new IOException(e.getMessage(), e);
                }
                executionMonitor.setProgress(container.size() / (double) rowsOverall);
            }
        }
    }

    /**
     * Creates a single row from the given protobuf table message.
     *
     * @param table
     *            The protobuf table message
     * @param index
     *            Index of the row in the table message
     * @return Data row based on the selected row in the protobuf table
     * @throws IOException
     *             If an error occured
     */
    private static DataRow createRow(final Table table, final int index, final FileStoreFactory fileStoreFactory, final PythonToKnimeExtensions pythonToKnimeExtensions) throws IOException {
        final DataCell[] cells = new DataCell[table.getNumCols()];
        final List<ColumnReference> colRefList = table.getColRefList();
        for (int i = 0; i < colRefList.size(); i++) {
            final String colType = colRefList.get(i).getType();
            if (colType.equals("BooleanColumn")) {
                final BooleanColumn column = table.getBooleanCol(colRefList.get(i).getIndexInType());
                final Table.BooleanValue value = column.getBooleanValue(index);
                cells[i] = value.hasValue() ? BooleanCellFactory.create(value.getValue()) : new MissingCell(null);
            } else if (colType.equals("IntegerColumn")) {
                final IntegerColumn column = table.getIntegerCol(colRefList.get(i).getIndexInType());
                final Table.IntegerValue value = column.getIntegerValue(index);
                cells[i] = value.hasValue() ? new IntCell(value.getValue()) : new MissingCell(null);
            } else if (colType.equals("LongColumn")) {
                final LongColumn column = table.getLongCol(colRefList.get(i).getIndexInType());
                final Table.LongValue value = column.getLongValue(index);
                cells[i] = value.hasValue() ? new LongCell(value.getValue()) : new MissingCell(null);
            } else if (colType.equals("DoubleColumn")) {
                final DoubleColumn column = table.getDoubleCol(colRefList.get(i).getIndexInType());
                final Table.DoubleValue value = column.getDoubleValue(index);
                cells[i] = value.hasValue() ? new DoubleCell(value.getValue()) : new MissingCell(null);
            } else if (colType.equals("StringColumn")) {
                final StringColumn column = table.getStringCol(colRefList.get(i).getIndexInType());
                final Table.StringValue value = column.getStringValue(index);
                cells[i] = value.hasValue() ? new StringCell(value.getValue()) : new MissingCell(null);
            } else if (colType.equals("DateAndTimeColumn")) {
                final DateAndTimeColumn column = table.getDateAndTimeCol(colRefList.get(i).getIndexInType());
                final Table.DateAndTimeValue value = column.getDateAndTimeValue(index);
                cells[i] = cellFromDateValue(value);
            } else if (colType.equals("BooleanListColumn")) {
                final BooleanListColumn column = table.getBooleanListCol(colRefList.get(i).getIndexInType());
                final Table.BooleanListValue value = column.getBooleanListValue(index);
                if (value.getIsMissing()) {
                    cells[i] = new MissingCell(null);
                } else {
                    final List<DataCell> singleCells = new ArrayList<DataCell>();
                    for (final Table.BooleanValue singleValue : value.getValueList()) {
                        singleCells.add(singleValue.hasValue() ? BooleanCellFactory.create(singleValue.getValue())
                            : new MissingCell(null));
                    }
                    cells[i] = column.getIsSet() ? CollectionCellFactory.createSetCell(singleCells)
                        : CollectionCellFactory.createListCell(singleCells);
                }
            } else if (colType.equals("IntegerListColumn")) {
                final IntegerListColumn column = table.getIntegerListCol(colRefList.get(i).getIndexInType());
                final Table.IntegerListValue value = column.getIntegerListValue(index);
                if (value.getIsMissing()) {
                    cells[i] = new MissingCell(null);
                } else {
                    final List<DataCell> singleCells = new ArrayList<DataCell>();
                    for (final Table.IntegerValue singleValue : value.getValueList()) {
                        singleCells.add(singleValue.hasValue() ? new IntCell(singleValue.getValue()) : new MissingCell(
                            null));
                    }
                    cells[i] = column.getIsSet() ? CollectionCellFactory.createSetCell(singleCells)
                        : CollectionCellFactory.createListCell(singleCells);
                }
            } else if (colType.equals("LongListColumn")) {
                final LongListColumn column = table.getLongListCol(colRefList.get(i).getIndexInType());
                final Table.LongListValue value = column.getLongListValue(index);
                if (value.getIsMissing()) {
                    cells[i] = new MissingCell(null);
                } else {
                    final List<DataCell> singleCells = new ArrayList<DataCell>();
                    for (final Table.LongValue singleValue : value.getValueList()) {
                        singleCells.add(singleValue.hasValue() ? new LongCell(singleValue.getValue())
                            : new MissingCell(null));
                    }
                    cells[i] = column.getIsSet() ? CollectionCellFactory.createSetCell(singleCells)
                        : CollectionCellFactory.createListCell(singleCells);
                }
            } else if (colType.equals("DoubleListColumn")) {
                final DoubleListColumn column = table.getDoubleListCol(colRefList.get(i).getIndexInType());
                final Table.DoubleListValue value = column.getDoubleListValue(index);
                if (value.getIsMissing()) {
                    cells[i] = new MissingCell(null);
                } else {
                    final List<DataCell> singleCells = new ArrayList<DataCell>();
                    for (final Table.DoubleValue singleValue : value.getValueList()) {
                        singleCells.add(singleValue.hasValue() ? new DoubleCell(singleValue.getValue())
                            : new MissingCell(null));
                    }
                    cells[i] = column.getIsSet() ? CollectionCellFactory.createSetCell(singleCells)
                        : CollectionCellFactory.createListCell(singleCells);
                }
            } else if (colType.equals("DateAndTimeListColumn")) {
                final DateAndTimeListColumn column = table.getDateAndTimeListCol(colRefList.get(i).getIndexInType());
                final Table.DateAndTimeListValue value = column.getDateAndTimeListValue(index);
                if (value.getIsMissing()) {
                    cells[i] = new MissingCell(null);
                } else {
                    final List<DataCell> singleCells = new ArrayList<DataCell>();
                    for (final Table.DateAndTimeValue singleValue : value.getValueList()) {
                        singleCells.add(cellFromDateValue(singleValue));
                    }
                    cells[i] = column.getIsSet() ? CollectionCellFactory.createSetCell(singleCells)
                        : CollectionCellFactory.createListCell(singleCells);
                }
            } else if (colType.equals("StringListColumn")) {
                final StringListColumn column = table.getStringListCol(colRefList.get(i).getIndexInType());
                final Table.StringListValue value = column.getStringListValue(index);
                if (value.getIsMissing()) {
                    cells[i] = new MissingCell(null);
                } else {
                    final List<DataCell> singleCells = new ArrayList<DataCell>();
                    for (final Table.StringValue singleValue : value.getValueList()) {
                        singleCells.add(singleValue.hasValue() ? new StringCell(singleValue.getValue())
                            : new MissingCell(null));
                    }
                    cells[i] = column.getIsSet() ? CollectionCellFactory.createSetCell(singleCells)
                        : CollectionCellFactory.createListCell(singleCells);
                }
            } else if (colType.equals("ObjectListColumn")) {
                final ObjectListColumn column = table.getObjectListCol(colRefList.get(i).getIndexInType());
                final Table.ObjectListValue value = column.getObjectListValue(index);
                if (value.getIsMissing()) {
                    cells[i] = new MissingCell(null);
                } else {
                    final List<DataCell> singleCells = new ArrayList<DataCell>();
                    for (final Table.ObjectValue singleValue : value.getValueList()) {
                        final Deserializer deserializer = pythonToKnimeExtensions.getDeserializer(PythonToKnimeExtensions.getExtension(column.getType()).getId());
                        singleCells.add(singleValue.hasValue() ? deserializer
                            .deserialize(singleValue.getValue().toByteArray(), fileStoreFactory) : new MissingCell(null));
                    }
                    cells[i] = column.getIsSet() ? CollectionCellFactory.createSetCell(singleCells)
                        : CollectionCellFactory.createListCell(singleCells);
                }
            } else if (colType.equals("ObjectColumn")) {
                final ObjectColumn column = table.getObjectCol(colRefList.get(i).getIndexInType());
                final Table.ObjectValue value = column.getObjectValue(index);
                final Deserializer deserializer = pythonToKnimeExtensions.getDeserializer(PythonToKnimeExtensions.getExtension(column.getType()).getId());
                cells[i] = value.hasValue() ? deserializer.deserialize(value.getValue().toByteArray(), fileStoreFactory) : new MissingCell(
                    null);
            }
        }
        return new DefaultRow(table.getRowId(index), cells);
    }

    /**
     * Creates a protobuf date and time value based on the given cell.
     *
     * @param cell
     *            The cell containing a date or a missing cell
     * @return The corresponding date and time value
     */
    private static Table.DateAndTimeValue dateValueFromCell(final DataCell cell) {
        final Table.DateAndTimeValue.Builder dateAndTimeValueBuilder = Table.DateAndTimeValue.newBuilder();
        if (!cell.isMissing()) {
            final DateAndTimeValue value = (DateAndTimeValue) cell;
            if (value.getUTCCalendarClone().get(Calendar.ERA) == GregorianCalendar.AD) {
                dateAndTimeValueBuilder.setYear(value.getYear());
                dateAndTimeValueBuilder.setMonth(value.getMonth() + 1);
                dateAndTimeValueBuilder.setDay(value.getDayOfMonth());
                dateAndTimeValueBuilder.setHour(value.getHourOfDay());
                dateAndTimeValueBuilder.setMinute(value.getMinute());
                dateAndTimeValueBuilder.setSecond(value.getSecond());
                dateAndTimeValueBuilder.setMillisecond(value.getMillis());
            }
        }
        return dateAndTimeValueBuilder.build();
    }

    /**
     * Creates a cell based on the given protobuf date and time value.
     *
     * @param value
     *            The value containing the date
     * @return A {@link DateAndTimeCell} or {@link MissingCell}
     */
    private static DataCell cellFromDateValue(final Table.DateAndTimeValue value) {
        if (value.hasYear()) {
            return new DateAndTimeCell(value.getYear(), value.getMonth() - 1, value.getDay(), value.getHour(),
                value.getMinute(), value.getSecond(), value.getMillisecond());
        }
        return new MissingCell(null);
    }

}
