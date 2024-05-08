package io.vena.bosk.updates;

public sealed interface UnconditionalUpdate<T> extends Update<T> permits Delete, Replace { }
