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
package com.netflix.hollow.tools.split;

import com.netflix.hollow.core.index.key.HollowPrimaryKeyValueDeriver;
import com.netflix.hollow.core.index.key.PrimaryKey;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.core.read.engine.HollowTypeReadState;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HollowSplitterPrimaryKeyCopyDirector implements HollowSplitterCopyDirector {
    
    private final int numShards;
    private final List<String> topLevelTypes;
    private final Map<String, HollowPrimaryKeyValueDeriver> primaryKeyDeriverByType;
    
    public HollowSplitterPrimaryKeyCopyDirector(HollowReadStateEngine stateEngine, int numShards, PrimaryKey... keys) {
        this.numShards = numShards;
        this.topLevelTypes = new ArrayList<>(keys.length);
        this.primaryKeyDeriverByType = new HashMap<>();
        
        for(int i=0;i<keys.length;i++) {
            topLevelTypes.add(keys[i].getType());
            HollowPrimaryKeyValueDeriver deriver = new HollowPrimaryKeyValueDeriver(keys[i], stateEngine);
            primaryKeyDeriverByType.put(keys[i].getType(), deriver);
        }
    }
    
    public void addReplicatedTypes(String... replicatedTypes) {
        topLevelTypes.addAll(Arrays.asList(replicatedTypes));
    }

    @Override
    public String[] getTopLevelTypes() {
        return topLevelTypes.toArray(new String[topLevelTypes.size()]);
    }

    @Override
    public int getNumShards() {
        return numShards;
    }

    @Override
    public int getShard(HollowTypeReadState topLevelType, int ordinal) {
        HollowPrimaryKeyValueDeriver deriver = primaryKeyDeriverByType.get(topLevelType.getSchema().getName());

        if (deriver == null) {
            return -1;
        }
        
        Object[] key = deriver.getRecordKey(ordinal);
        
        return hashKey(topLevelType.getSchema().getName(), key) % numShards;
    }
    
    public int hashKey(String type, Object[] key) {
        return Arrays.hashCode(key);
    }

}
