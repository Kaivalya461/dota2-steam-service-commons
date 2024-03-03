package com.kv.service;

import com.kv.dao.MatchDetailsDaoFileStorageImpl;
import com.kv.matchdetails.dto.MatchDetailsDto;
import com.kv.matchdetails.dto.MatchesDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class CommonUtilityService {

    private static final short FIVE_AND_HALF_HOURS_TIME_IN_SECONDS = 19800;
    @Autowired
    private MatchDetailsDaoFileStorageImpl matchDetailsDao;

    public Object getDataFromDB(Object id) {
        if(matchDetailsDao.dataSourceEnabled)
            return matchDetailsDao.findByMatchId(id);
        else
            return null;
    }

    public Object saveDataToDB(MatchDetailsDto obj) {
        if(matchDetailsDao.dataSourceEnabled)
            return matchDetailsDao.save(obj);
        else
            return null;
    }

    public static Set<MatchesDto> getMatchesPlayedInBetween(LocalDate fromDate, LocalDate toDate, Collection<MatchesDto> matchesDtoCollection) {
        return matchesDtoCollection
                .stream()
                .filter(match -> (fromDate.isBefore(LocalDateTime.ofEpochSecond(match.getStart_time(), 0, ZoneOffset.ofTotalSeconds(FIVE_AND_HALF_HOURS_TIME_IN_SECONDS)).toLocalDate()) || fromDate.isEqual(LocalDateTime.ofEpochSecond(match.getStart_time(), 0, ZoneOffset.ofTotalSeconds(FIVE_AND_HALF_HOURS_TIME_IN_SECONDS)).toLocalDate())
                        && (toDate.isAfter(LocalDateTime.ofEpochSecond(match.getStart_time(), 0, ZoneOffset.ofTotalSeconds(FIVE_AND_HALF_HOURS_TIME_IN_SECONDS)).toLocalDate()) || toDate.isEqual(LocalDateTime.ofEpochSecond(match.getStart_time(), 0, ZoneOffset.ofTotalSeconds(FIVE_AND_HALF_HOURS_TIME_IN_SECONDS)).toLocalDate()))))
                .collect(Collectors.toSet());
    }

    public static ZonedDateTime convertToZonedUtc(long dateTimeInEpoch) {
        Instant i = Instant.ofEpochSecond(dateTimeInEpoch);
        return ZonedDateTime.ofInstant(i, ZoneId.of("UTC"));
    }

    public static boolean hasPlayerWonTheMatch(boolean radiantWin, int playerSlot) {
        if(isPlayerInRadiantTeam(playerSlot))
            return radiantWin;

        return !radiantWin;
    }

    /**
     * Logic For Identifying Player Team ->
     * Radiant Team has Player slots starting from 0,1,2,3,4
     * Dire Team has Player slots starting from 128,129,130,131,132
    * */
    public static boolean isPlayerInRadiantTeam(int playerSlot) {
        return playerSlot < 128;
    }
}
