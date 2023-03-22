package org.example.utils;

import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MessageUtil {

    private static final String ORDER_PROPERTY = "chunkIndex";

    public static int getOrder(Message message) {
        String orderString = message.getProperty(ORDER_PROPERTY);
        if (orderString == null) {
            throw new IllegalArgumentException("Message does not have an order property.");
        }
        return Integer.parseInt(orderString);
    }

    public static List<Message> splitMessage(Message message, int chunkSize,String key) {
        byte[] payload = message.getBody();
        int payloadSize = payload.length;

        if (payloadSize <= chunkSize) {
            return Collections.singletonList(message);
        }

        int numOfChunks = (payloadSize + chunkSize - 1) / chunkSize;
        List<Message> messageChunks = new ArrayList<>(numOfChunks);

        for (int i = 0; i < numOfChunks; i++) {
            int startIndex = i * chunkSize;
            int endIndex = Math.min(startIndex + chunkSize, payloadSize);
            byte[] chunkPayload = new byte[endIndex - startIndex];
            System.arraycopy(payload, startIndex, chunkPayload, 0, chunkPayload.length);

            Message chunkMessage = new Message(message.getTopic(), message.getTags(), key,chunkPayload);
            chunkMessage.putUserProperty("chunkIndex", String.valueOf(i));
            chunkMessage.putUserProperty("numOfChunks", String.valueOf(numOfChunks));
            messageChunks.add(chunkMessage);
        }

        return messageChunks;
    }

    public static Message assembleMessage(List<Message> messageChunks) {
        int numOfChunks = messageChunks.size();
        int totalPayloadSize = 0;

        for (Message chunk : messageChunks) {
            totalPayloadSize += chunk.getBody().length;
        }

        byte[] assembledPayload = new byte[totalPayloadSize];
        int destPos = 0;

        for (Message chunk : messageChunks) {
            byte[] chunkPayload = chunk.getBody();
            System.arraycopy(chunkPayload, 0, assembledPayload, destPos, chunkPayload.length);
            destPos += chunkPayload.length;
        }

        Message assembledMessage = new Message(messageChunks.get(0).getTopic(), messageChunks.get(0).getTags(), assembledPayload);
        return assembledMessage;
    }
}
