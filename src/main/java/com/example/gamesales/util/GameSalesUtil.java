package com.example.gamesales.util;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class GameSalesUtil {

    public static File multipartToFile(MultipartFile multipartFile) throws IOException {
        File file = new File(System.getProperty("java.io.tmpdir") + File.separator + multipartFile.getOriginalFilename());
        multipartFile.transferTo(file);
        return file;
    }

    public static String populateErrorMessage(String errorMessage, String appendedMessage) {

        if (StringUtils.isEmpty(errorMessage)) {
            return appendedMessage;
        }
        return errorMessage.concat(", " + appendedMessage);
    }

    public static Date convertToDate(LocalDateTime localDateTime) {
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static LocalDateTime convertTimestampToLocalDateTime(Timestamp timestamp) {
        return LocalDateTime.ofInstant(timestamp.toInstant(), ZoneId.systemDefault());
    }
}
