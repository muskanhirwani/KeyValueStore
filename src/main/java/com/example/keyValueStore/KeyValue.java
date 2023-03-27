package com.example.keyValueStore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeyValue {
//    String keyValue;
    String value;
    String condition;
//    String expiryTime;
    private long expiry;
    private boolean expired;


    public void setExpiry(long expiry) {
        this.expiry = expiry;
    }

    public long getExpiry() {
        return expiry;
    }

    public boolean isExpired() {
        return expired;
    }

    public void setExpired(boolean expired) {
        this.expired = expired;
    }

    public void push(String[] values) {

    }
}
