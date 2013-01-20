package es.ynel.mongofs.api.config;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Config
{
	private static ApplicationContext appContext;
	
	public static <T> T getBean(Class<T> clazz)
	{
		if (appContext == null)
		{
			appContext = new ClassPathXmlApplicationContext("applicationContext.xml");
		}
		
		return appContext.getBean(clazz);
	}
	
}
