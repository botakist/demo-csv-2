package com.example.gamesales.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class CustomLocalDateTimeSerializer extends StdSerializer<LocalDateTime> {

    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public CustomLocalDateTimeSerializer() {
        this(null);
    }

    public CustomLocalDateTimeSerializer(Class<LocalDateTime> t) {
        super(t);
    }

    @Override
    public void serialize(
            LocalDateTime value,
            JsonGenerator gen,
            SerializerProvider arg2)
            throws IOException {

        gen.writeString(formatter.format(value.atZone(ZoneId.systemDefault())));
    }
}
