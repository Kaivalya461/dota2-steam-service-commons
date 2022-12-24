package com.kv.service;

import com.kv.dao.MatchDetailsDaoFileStorageImpl;
import com.kv.matchdetails.dto.MatchDetailsDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CommonUtilityService {
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
}
