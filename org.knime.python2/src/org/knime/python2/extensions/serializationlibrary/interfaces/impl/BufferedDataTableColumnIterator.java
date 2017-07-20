/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 * ---------------------------------------------------------------------
 *
 * History
 *   Jul 20, 2017 (clemens): created
 */
package org.knime.python2.extensions.serializationlibrary.interfaces.impl;

import java.io.IOException;

import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.StringValue;
import org.knime.core.data.collection.CollectionDataValue;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.python.typeextension.KnimeToPythonExtensions;
import org.knime.python.typeextension.Serializer;
import org.knime.python2.extensions.serializationlibrary.column.impl.BooleanCollectionColumn;
import org.knime.python2.extensions.serializationlibrary.column.impl.BooleanColumn;
import org.knime.python2.extensions.serializationlibrary.column.impl.BytesCollectionColumn;
import org.knime.python2.extensions.serializationlibrary.column.impl.BytesColumn;
import org.knime.python2.extensions.serializationlibrary.column.impl.DoubleCollectionColumn;
import org.knime.python2.extensions.serializationlibrary.column.impl.DoubleColumn;
import org.knime.python2.extensions.serializationlibrary.column.impl.IntCollectionColumn;
import org.knime.python2.extensions.serializationlibrary.column.impl.IntColumn;
import org.knime.python2.extensions.serializationlibrary.column.impl.LongCollectionColumn;
import org.knime.python2.extensions.serializationlibrary.column.impl.LongColumn;
import org.knime.python2.extensions.serializationlibrary.column.impl.MissingPossibleColumn;
import org.knime.python2.extensions.serializationlibrary.column.impl.StringCollectionColumn;
import org.knime.python2.extensions.serializationlibrary.column.impl.StringColumn;
import org.knime.python2.extensions.serializationlibrary.column.interfaces.Column;
import org.knime.python2.extensions.serializationlibrary.column.interfaces.TableColumnIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;

/**
 *
 * @author clemens
 */
public class BufferedDataTableColumnIterator implements TableColumnIterator {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(BufferedDataTableColumnIterator.class);

    private final int m_numberRows;

    private final CloseableRowIterator m_iterator;

    private final TableSpec m_spec;

    private final KnimeToPythonExtensions m_knimeToPythonExtensions;

    private final ExecutionMonitor m_executionMonitor;

    private final BufferedDataTableChunker.IterationProperties m_iterIterationProperties;

    private Column[] m_cols;

    private StringColumn m_indexCol;

    private int m_colIdx;

    /**
     * Constructor.
     *
     * @param spec the spec of the table to chunk in the python table representation format
     * @param rowIterator an iterator for the table to chunk
     * @param numberRows the number of rows of the table to chunk
     * @param monitor an execution monitor for reporting progress
     * @param ip iteration properties shared with the associated chunker to ensure a consistent state
     */
    public BufferedDataTableColumnIterator(final TableSpec spec, final CloseableRowIterator rowIterator, final int numberRows,
        final ExecutionMonitor monitor, final BufferedDataTableChunker.IterationProperties ip) {
        m_numberRows = numberRows;
        m_spec = spec;
        m_iterator = rowIterator;
        m_knimeToPythonExtensions = new KnimeToPythonExtensions();
        m_executionMonitor = monitor;
        m_iterIterationProperties = ip;
        m_cols = new Column[spec.getNumberColumns()];
        m_indexCol = new StringColumn(numberRows);
        m_colIdx = 0;
        int ctr = 0;
        for(Type type:spec.getColumnTypes()) {
            switch(type) {
                case BOOLEAN:
                    m_cols[ctr] = new BooleanColumn(spec.getNumberColumns());
                    break;
                case INTEGER:
                    m_cols[ctr] = new IntColumn(spec.getNumberColumns());
                    break;
                case LONG:
                    m_cols[ctr] = new LongColumn(spec.getNumberColumns());
                    break;
                case DOUBLE:
                    m_cols[ctr] = new DoubleColumn(spec.getNumberColumns());
                    break;
                case STRING:
                    m_cols[ctr] = new StringColumn(spec.getNumberColumns());
                    break;
                case BYTES:
                    m_cols[ctr] = new BytesColumn(spec.getNumberColumns());
                    break;
                case BOOLEAN_LIST:
                case BOOLEAN_SET:
                    m_cols[ctr] = new BooleanCollectionColumn(spec.getNumberColumns());
                    break;
                case INTEGER_LIST:
                case INTEGER_SET:
                    m_cols[ctr] = new IntCollectionColumn(spec.getNumberColumns());
                    break;
                case LONG_LIST:
                case LONG_SET:
                    m_cols[ctr] = new LongCollectionColumn(spec.getNumberColumns());
                    break;
                case DOUBLE_LIST:
                case DOUBLE_SET:
                    m_cols[ctr] = new DoubleCollectionColumn(spec.getNumberColumns());
                    break;
                case STRING_LIST:
                case STRING_SET:
                    m_cols[ctr] = new StringCollectionColumn(spec.getNumberColumns());
                    break;
                case BYTES_LIST:
                case BYTES_SET:
                    m_cols[ctr] = new BytesCollectionColumn(spec.getNumberColumns());
                    break;
                default:
                    throw new IllegalStateException("Tpye " + type + " is not known as a column tpye!");
            }
            ctr++;
        }
        ctr = 0;
        while(rowIterator.hasNext()) {
            DataRow row = rowIterator.next();
            //TODO rowKey
            for(int i=0; i<m_cols.length; i++) {
                DataCell dataCell = row.getCell(i);
                if(dataCell.isMissing()) {
                    ((MissingPossibleColumn)m_cols[i]).setMissing(ctr);
                }
                int innerCtr = 0;
                switch(spec.getColumnTypes()[i]) {
                    case BOOLEAN:
                        ((BooleanColumn) m_cols[i]).set(ctr, ((BooleanValue)dataCell).getBooleanValue());
                        break;
                    case INTEGER:
                        ((IntColumn) m_cols[i]).set(ctr, ((IntValue)dataCell).getIntValue());
                        break;
                    case LONG:
                        ((LongColumn) m_cols[i]).set(ctr, ((LongValue)dataCell).getLongValue());
                        break;
                    case DOUBLE:
                        ((DoubleColumn) m_cols[i]).set(ctr, ((DoubleValue)dataCell).getDoubleValue());
                        break;
                    case STRING:
                        ((StringColumn) m_cols[i]).set(ctr, ((StringValue)dataCell).getStringValue());
                        break;
                    case BYTES:
                        final Serializer serializer = m_knimeToPythonExtensions
                            .getSerializer(KnimeToPythonExtensions.getExtension(dataCell.getType()).getId());
                        try {
                            ((BytesColumn) m_cols[i]).set(ctr, serializer.serialize(dataCell));
                        } catch (IOException ex) {
                            LOGGER.error(ex.getMessage(), ex);
                            ((BytesColumn) m_cols[i]).setMissing(ctr);
                        }
                        break;
                    case BOOLEAN_LIST:
                    case BOOLEAN_SET:
                        final CollectionDataValue boolColCell = (CollectionDataValue)dataCell;
                        BooleanColumn boolCol = new BooleanColumn(boolColCell.size());
                        for (final DataCell innerCell : boolColCell) {
                           if(innerCell.isMissing()) {
                               boolCol.setMissing(innerCtr);
                           } else {
                               boolCol.set(innerCtr, ((BooleanValue)innerCell).getBooleanValue());
                           }
                           innerCtr++;
                        }
                        ((BooleanCollectionColumn) m_cols[i]).set(ctr, boolCol);
                        break;
                    case INTEGER_LIST:
                    case INTEGER_SET:
                        final CollectionDataValue intColCell = (CollectionDataValue)dataCell;
                        IntColumn intCol = new IntColumn(intColCell.size());
                        for (final DataCell innerCell : intColCell) {
                           if(innerCell.isMissing()) {
                               intCol.setMissing(innerCtr);
                           } else {
                               intCol.set(innerCtr, ((IntValue)innerCell).getIntValue());
                           }
                           innerCtr++;
                        }
                        ((IntCollectionColumn) m_cols[i]).set(ctr, intCol);
                        break;
                    case LONG_LIST:
                    case LONG_SET:
                        final CollectionDataValue longColCell = (CollectionDataValue)dataCell;
                        LongColumn longCol = new LongColumn(longColCell.size());
                        for (final DataCell innerCell : longColCell) {
                           if(innerCell.isMissing()) {
                               longCol.setMissing(innerCtr);
                           } else {
                               longCol.set(innerCtr, ((LongValue)innerCell).getLongValue());
                           }
                           innerCtr++;
                        }
                        ((LongCollectionColumn) m_cols[i]).set(ctr, longCol);
                        break;
                    case DOUBLE_LIST:
                    case DOUBLE_SET:
                        final CollectionDataValue doubleColCell = (CollectionDataValue)dataCell;
                        DoubleColumn doubleCol = new DoubleColumn(doubleColCell.size());
                        for (final DataCell innerCell : doubleColCell) {
                           if(innerCell.isMissing()) {
                               doubleCol.setMissing(innerCtr);
                           } else {
                               doubleCol.set(innerCtr, ((DoubleValue)innerCell).getDoubleValue());
                           }
                           innerCtr++;
                        }
                        ((DoubleCollectionColumn) m_cols[i]).set(ctr, doubleCol);
                        break;
                    case STRING_LIST:
                    case STRING_SET:
                        final CollectionDataValue stringColCell = (CollectionDataValue)dataCell;
                        StringColumn stringCol = new StringColumn(stringColCell.size());
                        for (final DataCell innerCell : stringColCell) {
                           if(innerCell.isMissing()) {
                               stringCol.setMissing(innerCtr);
                           } else {
                               stringCol.set(innerCtr, ((StringValue)innerCell).getStringValue());
                           }
                           innerCtr++;
                        }
                        ((StringCollectionColumn) m_cols[i]).set(ctr, stringCol);
                        break;
                    case BYTES_LIST:
                    case BYTES_SET:
                        final Serializer colSerializer = m_knimeToPythonExtensions
                            .getSerializer(KnimeToPythonExtensions.getExtension(dataCell.getType()).getId());
                        final CollectionDataValue bytesColCell = (CollectionDataValue)dataCell;
                        BytesColumn bytesCol = new BytesColumn(bytesColCell.size());
                        for (final DataCell innerCell : bytesColCell) {
                           if(innerCell.isMissing()) {
                               bytesCol.setMissing(innerCtr);
                           } else {
                               try {
                               bytesCol.set(innerCtr, colSerializer.serialize(innerCell));
                               } catch (IOException ex) {
                                   LOGGER.error(ex.getMessage(), ex);
                                   bytesCol.setMissing(innerCtr);
                               }
                           }
                           innerCtr++;
                        }
                        ((BytesCollectionColumn) m_cols[i]).set(ctr, bytesCol);
                        break;
                    default:
                        throw new IllegalStateException("Tpye " + spec.getColumnTypes()[i] + " is not known as a column tpye!");
                }
            }
            ctr++;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Column next() {
        Column c = m_cols[m_colIdx];
        m_colIdx++;
        return c;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return m_colIdx < m_spec.getNumberColumns();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberRemainingCols() {
        return m_colIdx - m_spec.getNumberColumns();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableSpec getTableSpec() {
        return m_spec;
    }

}
