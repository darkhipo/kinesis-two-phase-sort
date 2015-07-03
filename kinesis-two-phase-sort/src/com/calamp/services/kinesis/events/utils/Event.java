/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.calamp.services.kinesis.events.utils;

import java.io.IOException;
import java.util.Random;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Captures the key elements of a stock trade, such as the ticker symbol, price,
 * number of shares, the type of the trade (buy or sell), and an id uniquely identifying
 * the trade.
 */
public class Event {

    private final static ObjectMapper JSON = new ObjectMapper();
    static {
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Represents the type of the stock trade eg buy or sell.
     */
    public enum TradeType {
        BUY,
        SELL
    }

    private String tickerSymbol;
    private TradeType tradeType;
    private double price;
    private long quantity;
    private long id;
    private long myTime;
    private Random rand;

    public Event() {
    }

    public Event(String tickerSymbol, TradeType tradeType, double price, long quantity, long id) {
        this.tickerSymbol = tickerSymbol;
        this.tradeType = tradeType;
        this.price = price;
        this.quantity = quantity;
        this.id = id;
        this.rand = new Random();
        this.myTime = System.currentTimeMillis() + this.rand.nextInt(com.calamp.services.kinesis.events.utils.Parameters.minimumAgeMillis);
    }

    public String getTickerSymbol() {
        return tickerSymbol;
    }

    public TradeType getTradeType() {
        return tradeType;
    }

    public double getPrice() {
        return price;
    }

    public long getQuantity() {
        return quantity;
    }

    public long getId() {
        return id;
    }
    
    public long getMyTime() {
        return myTime;
    }

    public byte[] toJsonAsBytes() {
        try {
            return JSON.writeValueAsBytes(this);
        } catch (IOException e) {
            return null;
        }
    }

    public static Event fromJsonAsBytes(byte[] bytes) {
        try {
            return JSON.readValue(bytes, Event.class);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return String.format("%d ID %d: %s %d shares of %s for $%.02f",
                myTime, id, tradeType, quantity, tickerSymbol, price);
    }

}
