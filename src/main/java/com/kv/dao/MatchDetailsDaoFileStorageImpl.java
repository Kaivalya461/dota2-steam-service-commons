package com.kv.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kv.matchdetails.dto.MatchDetailsDto;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Supplier;

@Service
@Log4j2
public class MatchDetailsDaoFileStorageImpl implements MatchDetailsDao<MatchDetailsDto> {

    private static final String DOTA2_DB_SUB_DIRECTORY_NAME = "\\dota2\\MATCH_DETAILS";
    private static final String DOTA2_DB_SUB_DIRECTORY_NAME_FOR_LINUX = "/dota2/MATCH_DETAILS";

    private static final String HOST_OS = getHostOS();

    //    @Value(value = "${kv.datasource.url}")
    private String dbBaseUrl = null;

    private String fullyQualifiedDatabaseBasePath = null;

    public boolean dataSourceEnabled;

    @Autowired
    private ObjectMapper objectMapper;

    MatchDetailsDaoFileStorageImpl(@Autowired Environment environment) {
        this.dbBaseUrl = environment.getProperty("kv.datasource.url");
        this.dataSourceEnabled = this.dbBaseUrl != null;
        this.fullyQualifiedDatabaseBasePath = fileStorageBasePathSupplier.get();
    }

    @Override
    public Collection<MatchDetailsDto> findByMatchIds(Collection<MatchDetailsDto> matchIds) {
        return null;
    }

    @Override
    public MatchDetailsDto findByMatchId(Object matchId) {
        MatchDetailsDto matchDetailsDto = new MatchDetailsDto();
        File file = new File(getAbsoluteFilePath(matchId.toString()));

        if (file.exists()) {
            try {
                matchDetailsDto = objectMapper.readValue(file, MatchDetailsDto.class);
            } catch (JsonProcessingException e) {
                log.error("Error occurred while reading/processing json, {}", e.getMessage());
            } catch (IOException e) {
                log.error("Error occurred while reading File, {}", e.getMessage());
            }
        } else {
            log.info("No existing data found for matchId: {}", matchId);
            return null;
        }
        return matchDetailsDto;
    }

    @Override
    public MatchDetailsDto save(MatchDetailsDto matchDetailsDto) {
        long pk = matchDetailsDto.getMatch_id();
        File file = new File(getAbsoluteFilePath(String.valueOf(pk)));
        try {
            if (!file.exists()) {
                Boolean isNewFileCreated = file.createNewFile();
                FileWriter fw = new FileWriter(file);
                String jsonString = convertToJsonString(matchDetailsDto);
                fw.write(jsonString);
                fw.flush();
                fw.close();
                log.debug("JSON String: {}", jsonString);
            }
        } catch (IOException ioException) {
            log.error("Error occurred while saving file for filename: {}", pk);
            log.error("Error message {}", ioException.getMessage());

        }
        return matchDetailsDto;
    }

    private String getAbsoluteFilePath(String fileName) {
        if(HOST_OS.contains("Linux")) {
            return fullyQualifiedDatabaseBasePath + "/" + fileName + ".txt";
        } else {
            return fullyQualifiedDatabaseBasePath + "\\" + fileName + ".txt";
        }
    }

    private String convertToJsonString(Object object) {
        String jsonString = null;
        try {
            jsonString = objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return jsonString;
    }

    private static String getHostOS() {
        String hostOS = System.getProperty("os.name");
        log.info("FileStorageImpl::getHostOS Host OS: {}", hostOS);
        return hostOS;
    }

    /**
     * Following supplier provides Database path with subdirectory
     * ex: /home/db/dota2/MATCH_DETAILS/
     */
    Supplier<String> fileStorageBasePathSupplier = () -> {
        //For Linux(Docker deployments) need to create the directory before saving file
        if(HOST_OS.contains("Linux")) {
            File fileBaseDirectory = new File(dbBaseUrl + DOTA2_DB_SUB_DIRECTORY_NAME_FOR_LINUX);
            if(!fileBaseDirectory.isDirectory()) {
                boolean fileBaseDirectoryCreated = fileBaseDirectory.mkdirs();
            }
            return dbBaseUrl + DOTA2_DB_SUB_DIRECTORY_NAME_FOR_LINUX;
        } else {
            return dbBaseUrl + DOTA2_DB_SUB_DIRECTORY_NAME;
        }
    };
}
