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

import org.apache.commons.lang.ArrayUtils;
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
import org.knime.python2.extensions.serializationlibrary.column.impl.StringCollectionColumn;
import org.knime.python2.extensions.serializationlibrary.column.impl.StringColumn;
import org.knime.python2.extensions.serializationlibrary.column.interfaces.Column;
import org.knime.python2.extensions.serializationlibrary.column.interfaces.TableColumnIterator;
import org.knime.python2.extensions.serializationlibrary.interfaces.Row;
import org.knime.python2.extensions.serializationlibrary.interfaces.TableSpec;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;

/**
 *
 * @author clemens
 */
public class KeyValueColumnIterator implements TableColumnIterator {

    private final TableSpec m_spec;

    private Row m_row;

    private int m_colIdx;

    /**
     * Constructor.
     *
     * @param spec a python integration specific table spec
     * @param row the row containing the values
     */
    public KeyValueColumnIterator(final TableSpec spec, final Row row) {
        m_spec = spec;
        m_row = row;
        m_colIdx = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Column next() {
        Type type = m_spec.getColumnTypes()[m_colIdx];
        m_colIdx++;
        if(m_row.getCell(m_colIdx).isMissing()) {
            StringColumn col = new StringColumn(1);
            col.setMissing(0);
            return col;
        }
        int ctr = 0;
        switch(type) {
            case BOOLEAN:
                BooleanColumn boolCol = new BooleanColumn(1);
                boolCol.set(1, m_row.getCell(m_colIdx).getBooleanValue().booleanValue());
                return boolCol;
            case INTEGER:
                IntColumn intCol = new IntColumn(1);
                intCol.set(1, m_row.getCell(m_colIdx).getIntegerValue().intValue());
                return intCol;
            case LONG:
                LongColumn longCol = new LongColumn(1);
                longCol.set(1, m_row.getCell(m_colIdx).getLongValue().longValue());
                return longCol;
            case DOUBLE:
                DoubleColumn doubleCol = new DoubleColumn(1);
                doubleCol.set(1, m_row.getCell(m_colIdx).getDoubleValue().doubleValue());
                return doubleCol;
            case STRING:
                StringColumn stringCol = new StringColumn(1);
                stringCol.set(1, m_row.getCell(m_colIdx).getStringValue());
                return stringCol;
            case BYTES:
                BytesColumn bytesCol = new BytesColumn(1);
                bytesCol.set(1, ArrayUtils.toPrimitive(m_row.getCell(m_colIdx).getBytesValue()));
                return bytesCol;
            //TODO change cell impl to use columns
            case BOOLEAN_LIST:
            case BOOLEAN_SET:
                BooleanCollectionColumn boolCollectionCol = new BooleanCollectionColumn(1);
                Boolean[] boolArray = m_row.getCell(m_colIdx).getBooleanArrayValue();
                BooleanColumn tmpBoolcol = new BooleanColumn(boolArray.length);
                for(Boolean val:boolArray) {
                    tmpBoolcol.set(ctr, val.booleanValue());
                    ctr++;
                }
                boolCollectionCol.set(1, tmpBoolcol);
                return boolCollectionCol;
            case INTEGER_LIST:
            case INTEGER_SET:
                IntCollectionColumn intCollectionCol = new IntCollectionColumn(1);
                Integer[] intArray = m_row.getCell(m_colIdx).getIntegerArrayValue();
                IntColumn tmpIntcol = new IntColumn(intArray.length);
                for(Integer val:intArray) {
                    tmpIntcol.set(ctr, val.intValue());
                    ctr++;
                }
                intCollectionCol.set(1, tmpIntcol);
                return intCollectionCol;
            case LONG_LIST:
            case LONG_SET:
                LongCollectionColumn longCollectionCol = new LongCollectionColumn(1);
                Long[] longArray = m_row.getCell(m_colIdx).getLongArrayValue();
                LongColumn tmpLongCol = new LongColumn (longArray.length);
                for (Long val:longArray) {
                    tmpLongCol.set(ctr, val.longValue());
                    ctr++;
                }
                longCollectionCol.set(1, tmpLongCol);
                return longCollectionCol;
            case DOUBLE_LIST:
            case DOUBLE_SET:
                DoubleCollectionColumn doubleCollectionCol = new DoubleCollectionColumn(1);
                Double[] doubleArray = m_row.getCell(m_colIdx).getDoubleArrayValue();
                DoubleColumn tmpDoubleCol = new DoubleColumn (doubleArray.length);
                for (Double val:doubleArray) {
                    tmpDoubleCol.set(ctr, val.doubleValue());
                    ctr++;
                }
                doubleCollectionCol.set(1, tmpDoubleCol);
                return doubleCollectionCol;
            case STRING_LIST:
            case STRING_SET:
                StringCollectionColumn stringCollectionCol = new StringCollectionColumn(1);
                String[] stringArray = m_row.getCell(m_colIdx).getStringArrayValue();
                StringColumn tmpStringCol = new StringColumn (stringArray.length);
                for (String val:stringArray) {
                    tmpStringCol.set(ctr, val);
                    ctr++;
                }
                stringCollectionCol.set(1, tmpStringCol);
                return stringCollectionCol;
            case BYTES_LIST:
            case BYTES_SET:
                BytesCollectionColumn bytesCollectionCol = new BytesCollectionColumn(1);
                Byte[][] bytesArray = m_row.getCell(m_colIdx).getBytesArrayValue();
                BytesColumn tmpBytesCol = new BytesColumn (bytesArray.length);
                for (Byte[] val:bytesArray) {
                    tmpBytesCol.set(ctr, ArrayUtils.toPrimitive(val));
                    ctr++;
                }
                bytesCollectionCol.set(1, tmpBytesCol);
                return bytesCollectionCol;
            default:
                throw new IllegalStateException("Tpye " + m_spec.getColumnTypes()[m_colIdx] + " is not known as a column tpye!");
        }
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
