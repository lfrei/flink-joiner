package com.redbeard.flink.operator;

import com.jayway.jsonpath.JsonPath;
import org.apache.flink.api.java.functions.KeySelector;

public class PostalCodeSelector implements KeySelector<String, String> {

    @Override
    public String getKey(String value) {
        return JsonPath
                .parse(value)
                .read("$.postalCode");
    }
}
