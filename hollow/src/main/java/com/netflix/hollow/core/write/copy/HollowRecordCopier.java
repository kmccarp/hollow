/*
 *  Copyright 2016-2019 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.hollow.core.write.copy;

import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.core.read.engine.HollowTypeReadState;
import com.netflix.hollow.core.read.engine.list.HollowListTypeReadState;
import com.netflix.hollow.core.read.engine.map.HollowMapTypeReadState;
import com.netflix.hollow.core.read.engine.object.HollowObjectTypeReadState;
import com.netflix.hollow.core.read.engine.set.HollowSetTypeReadState;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.schema.HollowSchema;
import com.netflix.hollow.core.write.HollowWriteRecord;
import com.netflix.hollow.core.write.HollowWriteStateEngine;
import com.netflix.hollow.tools.combine.IdentityOrdinalRemapper;
import com.netflix.hollow.tools.combine.OrdinalRemapper;

/**
 * A HollowRecordCopier is used to copy records from a {@link HollowReadStateEngine} into a {@link HollowWriteStateEngine} 
 *
 */
public abstract class HollowRecordCopier {

    protected final HollowTypeReadState readTypeState;
    protected final HollowWriteRecord writeRecord;
    protected final OrdinalRemapper ordinalRemapper;
    protected final boolean preserveHashPositions;

    protected HollowRecordCopier(HollowTypeReadState readTypeState, HollowWriteRecord writeRecord, OrdinalRemapper ordinalRemapper, boolean preserveHashPositions) {
        this.readTypeState = readTypeState;
        this.writeRecord = writeRecord;
        this.ordinalRemapper = ordinalRemapper;
        this.preserveHashPositions = preserveHashPositions;
    }

    public HollowTypeReadState getReadTypeState() {
        return readTypeState;
    }

    public abstract HollowWriteRecord copy(int ordinal);

    public static HollowRecordCopier createCopier(HollowTypeReadState typeState) {
        return createCopier(typeState, typeState.getSchema());
    }

    public static HollowRecordCopier createCopier(HollowTypeReadState typeState, HollowSchema destinationSchema) {
        return createCopier(typeState, destinationSchema, IdentityOrdinalRemapper.INSTANCE, true);
    }

    public static HollowRecordCopier createCopier(HollowTypeReadState typeState, OrdinalRemapper remapper, boolean preserveHashPositions) {
        return createCopier(typeState, typeState.getSchema(), remapper, preserveHashPositions);
    }

    public static HollowRecordCopier createCopier(HollowTypeReadState typeState, HollowSchema destinationSchema, OrdinalRemapper ordinalRemapper, boolean preserveHashPositions) {
        if(typeState instanceof HollowObjectTypeReadState state)
            return new HollowObjectCopier(state, (HollowObjectSchema)destinationSchema, ordinalRemapper);
        if(typeState instanceof HollowListTypeReadState state)
            return new HollowListCopier(state, ordinalRemapper);
        if(typeState instanceof HollowSetTypeReadState state)
            return new HollowSetCopier(state, ordinalRemapper, preserveHashPositions);
        if(typeState instanceof HollowMapTypeReadState state)
            return new HollowMapCopier(state, ordinalRemapper, preserveHashPositions);

        throw new UnsupportedOperationException("I don't know how to create a copier for a " + typeState.getClass().getSimpleName());
    }
}
