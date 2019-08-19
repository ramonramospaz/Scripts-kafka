from kafka import KafkaProducer

class UsersNotificationsRequest:
	userNotificationRequest = []
	
	def __init__(self,ListUserNotification):
		self.userNotificationRequest=ListUserNotification

	def __str__(self):
		listUserNotificationRequest="{\"userNotificationRequest\":["
		for userNR in self.userNotificationRequest:
			listUserNotificationRequest+=str(userNR)+","
		return listUserNotificationRequest[:-1]+"]}"

class UserNotificationRequest:
	def __init__(self,notificationType,template,language,to,metadata):
		self.type=notificationType
		self.template=template
		self.language=language
		self.to=to
		self.metadata=metadata
	def __str__(self):
		return '{\"type\":\"'+self.type+'\",\"template\":\"'+self.template+'\",\"language\":\"'+self.language+'\",\"to\":\"'+self.to+'\", \"metadata\":{'+','.join("\"%s\":\"%r\"" % (key,val) for (key,val) in self.metadata.iteritems())+"}}"

metadata1={"Borrower.firstName":"Ramon","Loan.id":"1","Lender.fullName":"Karnasia"}

userNotificationRequest1=UserNotificationRequest("EMAIL","EMAIL_DELAY_BORROWER_NOTIFICATION","es_US","ramonramospaz@gmail.com",metadata1)

metadata2={"Borrower.firstName":"Ramon","Loan.id":"1","Lender.fullName":"Karnasia"}

userNotificationRequest2=UserNotificationRequest("SMS","SMS_DELAY_BORROWER_NOTIFICATION","es_US","+584126567959",metadata2)

metadata3={"Borrower.firstName":"Ramon","Lender.fullName":"Karnasia"}

userNotificationRequest3=UserNotificationRequest("EMAIL","EMAIL_DELAY_LENDER_NOTIFICATION","es_US","ramonramospaz@gmail.com",metadata3)

metadata4={"Borrower.firstName":"Ramon","Lender.fullName":"Karnasia"}

userNotificationRequest4=UserNotificationRequest("SMS","SMS_DELAY_LENDER_NOTIFICATION","es_US","+584126567959",metadata4)


mylist=[]
mylist.append(userNotificationRequest1)
mylist.append(userNotificationRequest2)
mylist.append(userNotificationRequest3)
mylist.append(userNotificationRequest4)

listUserNotification=UsersNotificationsRequest(mylist)

print "Object to send"
print str(listUserNotification)

print "Sending object to topic userNotification"
producer = KafkaProducer(bootstrap_servers="localhost:9092")

producer.send('userNotification',str(listUserNotification))
producer.flush()
