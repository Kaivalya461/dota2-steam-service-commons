package com.kv.service;

import com.kv.dao.MatchDetailsDaoFileStorageImpl;
import com.kv.matchdetails.dto.MatchDetailsDto;
import com.kv.matchdetails.dto.MatchesDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
}
