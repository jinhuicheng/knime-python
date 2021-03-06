/*
 * ------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------
 *
 * History
 *   Aug 17, 2017 (clemens): created
 */
package org.knime.python2.serde.flatbuffers.inserters;

import org.knime.python2.extensions.serializationlibrary.interfaces.Cell;
import org.knime.python2.extensions.serializationlibrary.interfaces.Type;
import org.knime.python2.serde.flatbuffers.flatc.ByteCell;
import org.knime.python2.serde.flatbuffers.flatc.ByteColumn;
import org.knime.python2.serde.flatbuffers.flatc.Column;

import com.google.flatbuffers.FlatBufferBuilder;

/**
 * Manages inserting a bytes column into the flatbuffers table.
 *
 * @author Clemens von Schwerin, KNIME GmbH, Konstanz, Germany
 */
public class BytesInserter implements FlatbuffersVectorInserter {

    private byte[][] m_values;
    private boolean[] m_missings;
    private int m_ctr;

    private String m_serializer;

    /**
     * Constructor.
     * @param numRows the number of rows in the table
     * @param serializer the serializer for the underlying type
     */
    public BytesInserter(final int numRows, final String serializer) {
        m_values = new byte[numRows][];
        m_missings = new boolean[numRows];
        m_serializer = serializer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(final Cell cell) {
        if(cell.isMissing()) {
            m_missings[m_ctr] = true;
        } else {
            m_values[m_ctr] = cell.getBytesValue();
        }
        m_ctr++;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int createColumn(final FlatBufferBuilder builder) {
        int[] bytesOffsets = new int[m_values.length];
        int ctr = 0;
        for (final byte[] bytes : m_values) {
            int byteCellOffset;
            if(bytes == null) {
                byteCellOffset = builder.createByteVector(new byte[0]);
            } else {
                byteCellOffset = builder.createByteVector(bytes);
            }
            bytesOffsets[ctr] = ByteCell.createByteCell(builder, byteCellOffset);
            ctr++;
        }
        final int valuesOffset = ByteColumn.createValuesVector(builder, bytesOffsets);

        final int missingOffset = ByteColumn.createMissingVector(builder, m_missings);

        final int colOffset = ByteColumn.createByteColumn(builder, builder.createString(m_serializer), valuesOffset,
            missingOffset);
        Column.startColumn(builder);
        Column.addType(builder, Type.BYTES.getId());
        Column.addByteColumn(builder, colOffset);
        return Column.endColumn(builder);
    }

}
