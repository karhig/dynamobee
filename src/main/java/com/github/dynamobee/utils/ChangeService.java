package com.github.dynamobee.utils;

import static java.util.Arrays.asList;

import com.github.dynamobee.changeset.ChangeEntry;
import com.github.dynamobee.changeset.ChangeLog;
import com.github.dynamobee.changeset.ChangeSet;
import com.github.dynamobee.exception.DynamobeeChangeSetException;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.reflections.Reflections;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;


/**
 * Utilities to deal with reflections and annotations
 */
public class ChangeService {
  private static final String DEFAULT_PROFILE = "default";

  private final String changeLogsBasePackage;
  private final List<String> activeProfiles;

  public ChangeService(String changeLogsBasePackage) {
    this(changeLogsBasePackage, null);
  }

  public ChangeService(String changeLogsBasePackage, Environment environment) {
    this.changeLogsBasePackage = changeLogsBasePackage;

    if (environment != null && environment.getActiveProfiles() != null && environment.getActiveProfiles().length > 0) {
      this.activeProfiles = asList(environment.getActiveProfiles());
    } else {
      this.activeProfiles = asList(DEFAULT_PROFILE);
    }
  }

  public List<Class<?>> fetchChangeLogs() {
    Reflections reflections = new Reflections(changeLogsBasePackage);
    Set<Class<?>> changeLogs = reflections.getTypesAnnotatedWith(ChangeLog.class); // TODO remove dependency, do own method
    List<Class<?>> filteredChangeLogs = (List<Class<?>>) filterByActiveProfiles(changeLogs);

    Collections.sort(filteredChangeLogs, new ChangeLogComparator());

    return filteredChangeLogs;
  }

  public List<Method> fetchChangeSets(final Class<?> type) throws DynamobeeChangeSetException {
    final List<Method> changeSets = filterChangeSetAnnotation(asList(type.getDeclaredMethods()));
    final List<Method> filteredChangeSets = (List<Method>) filterByActiveProfiles(changeSets);

    Collections.sort(filteredChangeSets, new ChangeSetComparator());

    return filteredChangeSets;
  }

  public boolean isRunAlwaysChangeSet(Method changesetMethod) {
    if (changesetMethod.isAnnotationPresent(ChangeSet.class)) {
      ChangeSet annotation = changesetMethod.getAnnotation(ChangeSet.class);
      return annotation.runAlways();
    } else {
      return false;
    }
  }

  public ChangeEntry createChangeEntry(Method changesetMethod) {
    if (changesetMethod.isAnnotationPresent(ChangeSet.class)) {
      ChangeSet annotation = changesetMethod.getAnnotation(ChangeSet.class);

      return ChangeEntry.builder()
          .setChangeId(annotation.id())
          .setAuthor(annotation.author())
          .setTimestamp(new Date())
          .setChangeLogClass(changesetMethod.getDeclaringClass().getName())
          .setChangeSetMethodName(changesetMethod.getName())
          .build();
    } else {
      return null;
    }
  }

  private boolean matchesActiveSpringProfile(AnnotatedElement element) {
    if (!ClassUtils.isPresent("org.springframework.context.annotation.Profile", null)) {
      return true;
    }

    List<String> profiles = profileAnnotations(element).stream().
        map(Profile::value).
        flatMap(Stream::of).
        collect(Collectors.toList());

    if (profiles.isEmpty()) {
      return true; // no-profiled changeset always matches
    }

    for (String profile : profiles) {
      if (profile != null && profile.length() > 0 && profile.charAt(0) == '!') {
        if (!activeProfiles.contains(profile.substring(1))) {
          return true;
        }
      } else if (activeProfiles.contains(profile)) {
        return true;
      }
    }
    return false;
  }

  private List<?> filterByActiveProfiles(Collection<? extends AnnotatedElement> annotated) {
    List<AnnotatedElement> filtered = new ArrayList<>();
    for (AnnotatedElement element : annotated) {
      if (matchesActiveSpringProfile(element)) {
        filtered.add(element);
      }
    }
    return filtered;
  }

  private List<Method> filterChangeSetAnnotation(List<Method> allMethods) throws DynamobeeChangeSetException {
    final Set<String> changeSetIds = new HashSet<>();
    final List<Method> changesetMethods = new ArrayList<>();
    for (final Method method : allMethods) {
      if (method.isAnnotationPresent(ChangeSet.class)) {
        String id = method.getAnnotation(ChangeSet.class).id();
        if (changeSetIds.contains(id)) {
          throw new DynamobeeChangeSetException(String.format("Duplicated changeset id found: '%s'", id));
        }
        changeSetIds.add(id);
        changesetMethods.add(method);
      }
    }
    return changesetMethods;
  }

  private List<Profile> profileAnnotations(AnnotatedElement element) {
    return Stream.of(element.getAnnotations()).
        map(this::getProfile).
        filter((p) -> p != null).
        collect(Collectors.toList());
  }

  private Profile getProfile(Annotation annotation) {
    if (isProfileAnnotation(annotation)) {
      return (Profile) (annotation);
    } else {
      return Stream.of(annotation.annotationType().getAnnotations()).
          filter(this::isProfileAnnotation). //  Does only allow one level of meta-annotation
              map(this::getProfile).
          filter(p -> p != null).
          findFirst().
          orElse(null);
    }
  }

  private boolean isProfileAnnotation(Annotation annotation) {
    return annotation.annotationType().equals(Profile.class);
  }
}
