package com.redbeard.flink.operator;

import com.jayway.jsonpath.JsonPath;
import org.apache.flink.api.common.functions.JoinFunction;

public class AddressJoiner implements JoinFunction<String, String, String> {

    @Override
    public String join(String address, String postalCode) {
        String town = JsonPath
                .parse(postalCode)
                .read("$.town");

        return JsonPath.parse(address)
                .put("$", "town", town)
                .jsonString();
    }
}
