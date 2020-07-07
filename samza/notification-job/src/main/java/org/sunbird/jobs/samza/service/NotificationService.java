package org.sunbird.jobs.samza.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.samza.config.Config;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.sunbird.common.models.util.datasecurity.OneWayHashing;
import org.sunbird.jobs.samza.util.JSONUtils;
import org.sunbird.jobs.samza.util.JobLogger;
import org.sunbird.jobs.samza.util.NotificationEnum;
import org.sunbird.notification.beans.EmailConfig;
import org.sunbird.notification.beans.EmailRequest;
import org.sunbird.notification.beans.SMSConfig;
import org.sunbird.notification.email.service.IEmailFactory;
import org.sunbird.notification.email.service.IEmailService;
import org.sunbird.notification.email.service.impl.IEmailProviderFactory;
import org.sunbird.notification.fcm.provider.IFCMNotificationService;
import org.sunbird.notification.fcm.provider.NotificationFactory;
import org.sunbird.notification.fcm.providerImpl.FCMHttpNotificationServiceImpl;
import org.sunbird.notification.sms.provider.ISmsProvider;
import org.sunbird.notification.utils.FCMResponse;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.sunbird.jobs.samza.util.Constant;
import org.sunbird.notification.utils.SMSFactory;

/**
 * 
 * @author manzarul
 *
 */
public class NotificationService {
	ObjectMapper mapper = new ObjectMapper();
	private JobLogger Logger = new JobLogger(NotificationService.class);
	private Config appConfig = null;
	private String accountKey = null;
	private ISmsProvider smsProvider = null;
	private IEmailFactory emailFactory = null;
	private IEmailService emailService = null;
	private IFCMNotificationService ifcmNotificationService = NotificationFactory
			.getInstance(NotificationFactory.instanceType.httpClinet.name());
	private int maxIterations = 0;
	private static int MAXITERTIONCOUNT = 2;

	public void initialize(Config config) throws Exception {
		JSONUtils.loadProperties(config);
		appConfig = config;
		accountKey = appConfig.get(NotificationEnum.fcm_account_key.name());
    
    Logger.info("NotificationService:initialize: sms config:: sms auth key: "+ appConfig.get(NotificationEnum.sms_auth_key.name())+
      " sms default provider: "+appConfig.get(NotificationEnum.sms_default_sender.name()));
    Logger.info("NotificationService:initialize: email config:: email-server: "+ appConfig.get(NotificationEnum.mail_server_from_email.name())+
      " mail-username: "+appConfig.get(NotificationEnum.mail_server_username.name())+ " mail-password: "+appConfig.get(NotificationEnum.mail_server_password.name())+
      " mail-host: "+appConfig.get(NotificationEnum.mail_server_host.name())+" mail-port: "+appConfig.get(NotificationEnum.mail_server_port.name()));
		SMSConfig smsConfig = new SMSConfig(
						appConfig.get(NotificationEnum.sms_auth_key.name()), appConfig.get(NotificationEnum.sms_default_sender.name()));
		smsProvider = SMSFactory.getInstance("91SMS", smsConfig);
		emailFactory = new IEmailProviderFactory();
		emailService = emailFactory.create(
		        new EmailConfig(appConfig.get(NotificationEnum.mail_server_from_email.name()),
										appConfig.get(NotificationEnum.mail_server_username.name()),
                    appConfig.get(NotificationEnum.mail_server_password.name()),
										appConfig.get(NotificationEnum.mail_server_host.name()),
										appConfig.get(NotificationEnum.mail_server_port.name())));
		maxIterations = getMaxIterations();
		Logger.info("NotificationService:initialize: Service config initialized");
	}

	public void processMessage(Map<String, Object> message, MessageCollector collector) throws Exception {
		Logger.info("Account key:"+ accountKey);
		FCMHttpNotificationServiceImpl.setAccountKey(accountKey);
		String msgId = (String) message.get(Constant.MID);
		Map<String, Object> edataMap = (Map<String, Object>) message.get(Constant.EDATA);
		Map<String, Object> objectMap = (Map<String, Object>) message.get(Constant.OBJECT);
		String requestHash = "";
		boolean isSuccess = false;
		if (edataMap != null && edataMap.size() > 0) {
			String actionValue = (String) edataMap.get(Constant.ACTION);
			if (Constant.ACTION_NAME.equalsIgnoreCase(actionValue)) {
				Map<String, Object> requestMap = (Map<String, Object>) edataMap.get(Constant.REQUEST);
				requestHash = OneWayHashing.encryptVal(mapper.writeValueAsString(requestMap));
				if (!requestHash.equals((String) objectMap.get(Constant.ID))) {
					Logger.info("NotificationService:processMessage: hashValue is not matching - " + requestHash);
				} else {
					Map<String, Object> notificationMap = (Map<String, Object>) requestMap.get(Constant.NOTIFICATION);
					if(notificationMap.get(Constant.MODE).equals(NotificationEnum.phone.name())) {
						isSuccess = sendSmsNotification(notificationMap, msgId);
					} else if(notificationMap.get(Constant.MODE).equals(NotificationEnum.email.name())){
						isSuccess = sendEmailNotification(notificationMap);
					} else if(notificationMap.get(Constant.MODE).equals(NotificationEnum.device.name())) {
						isSuccess = notifyDevice(notificationMap);
					}
					if (isSuccess) {
						Logger.info("Notification sent successfully.");
					} else {
						Logger.info("Notification sent failure");
						handleFailureMessage(message, edataMap, collector);
					}
				}
			} else {
				Logger.info("NotificationService:processMessage action name is incorrect: " + actionValue + "for message id:"+msgId);
			}
		} else {
			Logger.info("NotificationService:processMessage event data map is either null or empty for message id:"+msgId);
		}
	}

	private void handleFailureMessage(Map<String, Object> message, Map<String, Object> edata, MessageCollector collector) {
		Logger.info("NotificationService:handleFailureMessage started");
		int iteration = (int) edata.get(NotificationEnum.iteration.name());
		if(iteration < maxIterations) {
			((Map<String, Object>) message.get(Constant.EDATA)).put(NotificationEnum.iteration.name(), iteration+1);
			collector.send(new OutgoingMessageEnvelope(
							new SystemStream(
											NotificationEnum.kafka.name(), appConfig.get("kafka.retry.topic")), message
			));
		}
	}

	protected int getMaxIterations() {
		maxIterations = appConfig.getInt("max.iteration.count.samza.job");
		if(maxIterations==0) {
			maxIterations = MAXITERTIONCOUNT;
		}
		return maxIterations;
	}

	private boolean sendEmailNotification(Map<String, Object> notificationMap) {
		List<String> emailIds = (List<String>) notificationMap.get(Constant.IDS);
		Map<String, Object> templateMap = (Map<String, Object>) notificationMap.get(Constant.TEMPLATE);
		Map<String, Object> config =  (Map<String, Object>) notificationMap.get(Constant.CONFIG);
		String subject = (String) config.get(Constant.SUBJECT);
		String emailText = (String) templateMap.get(Constant.DATA);
		EmailRequest emailRequest = new EmailRequest(subject, emailIds,null,null,"", emailText,null);
		return emailService.sendEmail(emailRequest);
	}

	private boolean sendSmsNotification(Map<String, Object> notificationMap, String msgId) {
		List<String> mobileNumbers = (List<String>) notificationMap.get(Constant.IDS);
		if (mobileNumbers != null) {
			Map<String, Object> templateMap = (Map<String, Object>) notificationMap.get(Constant.TEMPLATE);
			String smsText = (String) templateMap.get(Constant.DATA);
			return smsProvider.bulkSms(mobileNumbers, smsText);
		} else {
			Logger.info("mobile numbers not provided for message id:"+msgId);
			return true;
		}
	}

	private boolean notifyDevice(Map<String, Object> notificationMap) throws JsonProcessingException {
		String topic = null;
		FCMResponse response = null;
		List<String> deviceIds = (List<String>) notificationMap.get(Constant.IDS);
		Map<String, String> dataMap = new HashMap<String, String>();
		dataMap.put(Constant.RAW_DATA, mapper.writeValueAsString(notificationMap.get(Constant.RAW_DATA)));
		Logger.info("NotificationService:processMessage: calling send notification ");
		if (deviceIds != null ) {
			response = ifcmNotificationService.sendMultiDeviceNotification(deviceIds, dataMap, false);
		} else {
			Map<String, Object> configMap = (Map<String, Object>) notificationMap.get(Constant.CONFIG);
			topic = (String) configMap.getOrDefault(Constant.TOPIC, "");
			response = ifcmNotificationService.sendTopicNotification(topic, dataMap, false);
		}
		if (response != null) {
			Logger.info("Send device notiifcation response with canonicalId,ErrorMsg,successCount,FailureCount"
							+ response.getCanonical_ids() + "," + response.getError() + ", " + response.getSuccess() + " "
							+ response.getFailure());
			return true;
		} else {
			Logger.info("response is improper from fcm:" + response + "for device ids" + deviceIds + "or topic"+ topic);
			return false;
		}
	}

}
