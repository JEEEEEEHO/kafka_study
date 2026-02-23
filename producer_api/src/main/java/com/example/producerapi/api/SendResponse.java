package com.example.producerapi.api;

public record SendResponse(boolean ok, String topic, String sent) {}