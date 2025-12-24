package mes.config;


import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;


@Configuration
public class WebMvcConfig implements WebMvcConfigurer{

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        //registry.addInterceptor(new GuiHttpInterceptor()).addPathPatterns("/gui/*");
        //registry.addInterceptor(new ApiHttpInterceptor()).addPathPatterns("/Api/*");
    }

    @Override
    public void addCorsMappings(CorsRegistry registry){
        registry.addMapping("/**")
                .allowedOrigins(
                        "http://localhost:8037", "https://da.actascld.co.kr", "http://actascld.co.kr:8037/"
                )
                .allowedMethods("GET", "POST", "PUT", "DELETE", "HEAD")
                .allowedHeaders("*")
                .allowCredentials(true);
    }
    

}
