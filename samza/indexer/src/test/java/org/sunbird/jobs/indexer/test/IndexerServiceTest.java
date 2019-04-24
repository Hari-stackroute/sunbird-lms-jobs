package org.sunbird.jobs.indexer.test;

import static org.powermock.api.mockito.PowerMockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.sunbird.common.ElasticSearchUtil;
import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.jobs.samza.service.IndexerService;
import org.sunbird.models.Constants;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ IndexerService.class, ElasticSearchUtil.class, })
@PowerMockIgnore({ "javax.management.*" })
public class IndexerServiceTest {

  @Before
  public void beforeEachTest() {
    PowerMockito.mockStatic(ElasticSearchUtil.class);

  }

  @Test
  public void testMessageProcessSuccessForUpsert() {
    Map<String, Object> messageMap = createMessageMap();
    IndexerService service = new IndexerService();
    ProjectCommonException error = null;

    try {
      service.process(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }
    Assert.assertTrue(error == null);
  }

  @Test
  public void testMessageProcessSuccessForDelete() {
    Map<String, Object> messageMap = createMessageMap();
    messageMap.put(Constants.OPERATION_TYPE, Constants.DELETE);
    IndexerService service = new IndexerService();
    ProjectCommonException error = null;
    try {
      service.process(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }
    Assert.assertTrue(error == null);

  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMessageProcessFailureForUpsert() {
    when(ElasticSearchUtil.upsertData(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
        .thenThrow(throwException());
    Map<String, Object> messageMap = createMessageMap();
    IndexerService service = new IndexerService();
    ProjectCommonException error = null;
    try {
      service.process(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }
    Assert.assertTrue(error != null);

  }

  @Test
  public void testMessageProcessFailureForDelete() {
    when(ElasticSearchUtil.removeData(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenThrow(throwException());

    Map<String, Object> messageMap = createMessageMap();
    messageMap.put(Constants.OPERATION_TYPE, Constants.DELETE);
    IndexerService service = new IndexerService();
    ProjectCommonException error = null;
    try {
      service.process(messageMap);
    } catch (ProjectCommonException e) {
      error = e;
    }
    Assert.assertTrue(error != null);

  }

  private Map<String, Object> createMessageMap() {
    Map<String, Object> messageMap = new HashMap<>();
    messageMap.put(Constants.IDENTIFIER, "123456");
    messageMap.put(Constants.OPERATION_TYPE, Constants.UPSERT);
    messageMap.put(Constants.EVENT_TYPE, Constants.Transactional);
    messageMap.put(Constants.OBJECT_TYPE, Constants.LOCATION);
    Map<String, Object> event = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();
    Map<String, Object> name = new HashMap<>();
    Map<String, Object> id = new HashMap<>();
    name.put(Constants.NV, "BLR");
    id.put(Constants.NV, "0001");
    properties.put("name", name);
    properties.put("id", id);
    event.put("properties", properties);
    messageMap.put(Constants.EVENT, event);
    messageMap.put(Constants.ETS, 123456L);
    messageMap.put(Constants.USER_ID, "ANONYMOUS");
    messageMap.put(Constants.CREATED_ON, "1556018741532");
    return messageMap;
  }

  private ProjectCommonException throwException() {
    return new ProjectCommonException("", "", 0);

  }
}
