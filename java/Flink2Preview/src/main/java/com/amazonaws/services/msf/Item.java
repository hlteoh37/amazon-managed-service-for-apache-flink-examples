package com.amazonaws.services.msf;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Item {

    private String id;
    private int quantity;
    private int group;
    private String data;
    private String timestamp;
}
