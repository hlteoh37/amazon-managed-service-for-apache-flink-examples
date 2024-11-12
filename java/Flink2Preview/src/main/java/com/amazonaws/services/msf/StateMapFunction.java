package com.amazonaws.services.msf;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

public class StateMapFunction extends RichMapFunction<Item, Item> {

    private transient ValueState<Item> valueState;
    private transient MapState<Integer, Item> mapState;
    private transient ListState<Item> listState;

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<Item> stateDescriptor = new ValueStateDescriptor<>("value-state", Item.class);
        valueState = getRuntimeContext().getState(stateDescriptor);

        MapStateDescriptor<Integer, Item> mapStateDescriptor = new MapStateDescriptor<>("map-state", Integer.class, Item.class);
        mapState = getRuntimeContext().getMapState(mapStateDescriptor);

        ListStateDescriptor<Item> listStateDescriptor = new ListStateDescriptor<>("list-state", Item.class);
        listState = getRuntimeContext().getListState(listStateDescriptor);
    }
    @Override
    public Item map(Item item) throws Exception {
        valueState.update(item);
        mapState.put(item.getGroup(), item);
        listState.add(item);
        return item;
    }
}
