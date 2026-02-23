# build stage
FROM eclipse-temurin:17-jdk AS build
WORKDIR /app

# Gradle wrapper / 설정 먼저 복사 (캐시 효율)
COPY gradlew gradlew
COPY gradlew.bat gradlew.bat
COPY gradle gradle
COPY build.gradle build.gradle
COPY settings.gradle settings.gradle

# 소스 복사
COPY src src

# 빌드 (테스트는 일단 스킵 권장)
RUN ./gradlew clean bootJar -x test

# run stage
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/build/libs/*.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java","-jar","/app/app.jar"]