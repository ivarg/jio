package io.ivarg.jio;

import java.util.Map;

public class UserArgs {

  private final Map<String, String> map;

  public UserArgs() {
    this(Map.of());
  }

  public UserArgs(Map<String, String> map) {
    this.map = map;
  }

  public String get(String key) {
    return map.get(key);
  }

  @Override
  public String toString() {
    return map.toString();
  }
}
