package io.vena.bosk.bytecode;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import static io.vena.bosk.util.ReflectionHelpers.setAccessible;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.stream.Collectors.joining;
import static org.objectweb.asm.ClassWriter.COMPUTE_FRAMES;
import static org.objectweb.asm.Opcodes.ACC_FINAL;
import static org.objectweb.asm.Opcodes.ACC_PRIVATE;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.DUP;
import static org.objectweb.asm.Opcodes.GETFIELD;
import static org.objectweb.asm.Opcodes.IFEQ;
import static org.objectweb.asm.Opcodes.IFNE;
import static org.objectweb.asm.Opcodes.ILOAD;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.ISTORE;
import static org.objectweb.asm.Opcodes.NEW;
import static org.objectweb.asm.Opcodes.POP;
import static org.objectweb.asm.Opcodes.PUTFIELD;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.SWAP;
import static org.objectweb.asm.Opcodes.V1_8;
import static org.objectweb.asm.Type.getMethodDescriptor;

/**
 * Wrapper around ASM's {@link ClassWriter} to simplify things for our purposes.
 * Users of this should not need to import anything from org.objectweb.asm.
 *
 * @param <T> The interface class that resulting class will implement.
 */
public final class ClassBuilder<T> {
	private final Class<? extends T> supertype;
	private final ClassLoader parentClassLoader;
	private final String superClassName;
	private final String slashyName; // like "java/lang/Object"
	private final String dottyName;  // like "java.lang.Object"
	private final StackTraceElement sourceFileOrigin; // Where this ClassBuilder was instantiated
	private ClassVisitor classVisitor = null;
	private ClassWriter classWriter = null;
	private MethodBuilder currentMethod = null;

	private final List<CurriedField> curriedFields = new ArrayList<>();

	/**
	 * @param className The simple name of the generated class;
	 * 		the actual name will be given the prefix <code>GENERATED_</code> to identify it as not corresponding to any source file
	 * @param supertype A superclass or interface for the generated class to inherit
	 * @param parentClassLoader The classloader that should be used as the parent of the one we'll use
	 * 		to load the newly-compiled class.
	 * @param sourceFileOrigin Indicates the package in which the generated class should reside, and
	 * 		the source file to which all debug line number information should refer.
	 */
	public ClassBuilder(String className, Class<? extends T> supertype, ClassLoader parentClassLoader, StackTraceElement sourceFileOrigin) {
		this.supertype = supertype;
		this.parentClassLoader = parentClassLoader;
		if (supertype.isInterface()) {
			superClassName = Type.getInternalName(Object.class);
		} else {
			superClassName = Type.getInternalName(supertype);
		}
		String sourceDottyName = sourceFileOrigin.getClassName();
		this.dottyName = sourceDottyName.substring(0, sourceDottyName.lastIndexOf('.')) + ".GENERATED_" + className;
		this.slashyName = dottyName.replace('.', '/');
		this.sourceFileOrigin = sourceFileOrigin;
	}

	public void beginClass() {
		String[] interfaces;
		if (supertype.isInterface()) {
			interfaces = new String[]{Type.getInternalName(supertype)};
		} else {
			interfaces = new String[0];
		}
		this.classWriter = new ClassWriter(COMPUTE_FRAMES);
		this.classVisitor = classWriter;
		classVisitor.visit(V1_8, ACC_PUBLIC | ACC_FINAL | ACC_SUPER, slashyName, null, superClassName, interfaces);
		classVisitor.visitSource(sourceFileOrigin.getFileName(), null);
	}

	private void generateConstructor(StackTraceElement sourceFileOrigin) {
		String ctorParameterDescriptor = curriedFields.stream()
			.map(CurriedField::typeDescriptor)
			.collect(joining());
		MethodVisitor ctor = classVisitor.visitMethod(ACC_PUBLIC, "<init>", "(" + ctorParameterDescriptor + ")V", null, null);
		ctor.visitCode();
		Label label = new Label();
		ctor.visitLabel(label);
		ctor.visitLineNumber(sourceFileOrigin.getLineNumber(), label);
		ctor.visitVarInsn(ALOAD, 0);
		ctor.visitMethodInsn(INVOKESPECIAL, superClassName, "<init>", "()V", false);
		for (CurriedField field: curriedFields) {
			ctor.visitVarInsn(ALOAD, 0);
			ctor.visitVarInsn(ALOAD, field.slot());
			ctor.visitFieldInsn(PUTFIELD, slashyName, field.name(), field.typeDescriptor());
		}
		ctor.visitInsn(RETURN);
		ctor.visitMaxs(0, 0); // Computed automatically
		ctor.visitEnd();
	}

	public static StackTraceElement here() {
		return new Exception().getStackTrace()[1];
	}

	public void beginMethod(Method method) {
		if (currentMethod == null) {
			currentMethod = new MethodBuilder(method, getMethodDescriptor(method), classVisitor);
		} else {
			throw new IllegalStateException("Method already in progress: " + currentMethod.method);
		}
	}

	public void finishMethod() {
		currentMethod.buildMethod();
		currentMethod = null;
	}

	/**
	 * Emit a CHECKCAST: <a href="https://docs.oracle.com/javase/specs/jvms/se8/html/jvms-6.html#jvms-6.5.checkcast">...</a>
	 */
	public void castTo(Class<?> expectedType) {
		emitLineNumberInfo();
		methodVisitor().visitTypeInsn(CHECKCAST, Type.getInternalName(expectedType));
	}

	/**
	 * @return a {@link LocalVariable} representing a reference parameter at the
	 * given position, which is zero-based. Assumes all parameters are single-slot types.
	 */
	public LocalVariable parameter(int index) {
		if (0 <= index && index < currentMethod.numParameters) {
			return new LocalVariable(OBJECT_TYPE, index);
		} else {
			throw new IllegalStateException("No parameter #" + index);
		}
	}

	/**
	 * Emit ALOAD: <a href="https://docs.oracle.com/javase/specs/jvms/se8/html/jvms-6.html#jvms-6.5.aload">...</a>
	 */
	public void pushLocal(LocalVariable var) {
		beginPush();
		methodVisitor().visitVarInsn(var.type().getOpcode(ILOAD), var.slot());
	}

	/**
	 * Emit ASTORE: <a href="https://docs.oracle.com/javase/specs/jvms/se8/html/jvms-6.html#jvms-6.5.astore">...</a>
	 */
	public LocalVariable popToLocal() {
		return popToLocal(OBJECT_TYPE);
	}

	/**
	 * Emit the appropriate store opcode for the given type, which can be either
	 * {@link #OBJECT_TYPE} or else one of the primitive types ({@link Type#INT_TYPE} etc.).
	 */
	public LocalVariable popToLocal(Type type) {
		LocalVariable result = currentMethod.newLocal(type);
		methodVisitor().visitVarInsn(type.getOpcode(ISTORE), result.slot());
		endPop(type.getSize());
		return result;
	}

	/**
	 * Emit code to push the given object on the operand stack.
	 *
	 * <p>
	 * Implemented as a GETFIELD: <a href="https://docs.oracle.com/javase/specs/jvms/se8/html/jvms-6.html#jvms-6.5.getfield">...</a>
	 *
	 * @param name purely descriptive; doesn't need to be unique
	 * @param type the static type of the value (because the dynamic type might not
	 *             be accessible from the generated class)
	 */
	public void pushObject(String name, Object object, Class<?> type) {
		CurriedField field = curry(name, object, type);
		beginPush();
		methodVisitor().visitVarInsn(ALOAD, 0);
		methodVisitor().visitFieldInsn(GETFIELD, slashyName, field.name(), field.typeDescriptor());
	}

	private CurriedField curry(String name, Object object, Class<?> type) {
		type.cast(object);
		for (CurriedField candidate: curriedFields) {
			if (candidate.value() == object) {
				return candidate;
			}
		}

		int ctorParameterSlot = 1 + curriedFields.size();
		CurriedField result = new CurriedField(
			ctorParameterSlot,
			"CURRIED" + ctorParameterSlot + "_" + name,
			Type.getDescriptor(type),
			object);
		curriedFields.add(result);

		classVisitor.visitField(
			ACC_PRIVATE | ACC_FINAL,
			result.name(),
			result.typeDescriptor(),
			null, null
		).visitEnd();

		return result;
	}

	/**
	 * Emit LDC: <a href="https://docs.oracle.com/javase/specs/jvms/se8/html/jvms-6.html#jvms-6.5.ldc">...</a>
	 */
	public void pushInt(int value) {
		beginPush();
		methodVisitor().visitLdcInsn(value);
	}

	/**
	 * Emit LDC: <a href="https://docs.oracle.com/javase/specs/jvms/se8/html/jvms-6.html#jvms-6.5.ldc">...</a>
	 */
	public void pushString(String value) {
		beginPush();
		methodVisitor().visitLdcInsn(value);
	}

	/**
	 * Emit DUP: <a href="https://docs.oracle.com/javase/specs/jvms/se8/html/jvms-6.html#jvms-6.5.dup">...</a>
	 */
	public void dup() {
		beginPush();
		methodVisitor().visitInsn(DUP);
	}

	/**
	 * Emit SWAP: <a href="https://docs.oracle.com/javase/specs/jvms/se8/html/jvms-6.html#jvms-6.5.swap">...</a>
	 */
	public void swap() {
		methodVisitor().visitInsn(SWAP);
	}

	/**
	 * Pop the top item off the operand stack
	 */
	public void pop() {
		methodVisitor().visitInsn(POP);
		endPop(1);
	}

	/**
	 * Emit the appropriate INVOKE instruction for the given Method.
	 */
	public void invoke(Method method) {
		setAccessible(method); // Hmm, we seem to get IllegalAccessError even after doing this
		emitLineNumberInfo();
		Class<?> type = method.getDeclaringClass();
		String typeName = Type.getInternalName(type);
		String methodName = method.getName();
		String signature = getMethodDescriptor(method);
		Type methodType = Type.getType(method);
		int weird = methodType.getArgumentsAndReturnSizes();
		int argumentSlots = weird >> 2; // NOTE: This is off by 1 for static methods!
		int resultSlots = weird & 0x3;
		if (isStatic(method.getModifiers())) {
			argumentSlots -= 1; // Static methods have no "this" argument
			methodVisitor().visitMethodInsn(INVOKESTATIC, typeName, methodName, signature, false);
		} else if (type.isInterface()) {
			methodVisitor().visitMethodInsn(INVOKEINTERFACE, typeName, methodName, signature, true);
		} else {
			methodVisitor().visitMethodInsn(INVOKEVIRTUAL, typeName, methodName, signature, false);
		}
		endPop(argumentSlots - resultSlots);
	}

	/**
	 * Emit INVOKESPECIAL for the given Constructor.
	 */
	public void invoke(Constructor<?> ctor) {
		setAccessible(ctor); // Hmm, we seem to get IllegalAccessError even after doing this
		emitLineNumberInfo();
		String typeName = Type.getInternalName(ctor.getDeclaringClass());
		Type[] parameterTypes = Stream.of(ctor.getParameterTypes()).map(Type::getType).toArray(Type[]::new);
		String signature = getMethodDescriptor(Type.getType(void.class), parameterTypes);
		methodVisitor().visitMethodInsn(INVOKESPECIAL, typeName, "<init>", signature, false);
		endPop(ctor.getParameterCount());
	}

	/**
	 * Emit NEW for the given class.
	 */
	public void instantiate(Class<?> type) {
		methodVisitor().visitTypeInsn(NEW, Type.getInternalName(type));
	}

	@SuppressWarnings("SameParameterValue")
	private void branchAround(Runnable action, int opcode, int poppedSlots) {
		Label label = new Label();
		methodVisitor().visitJumpInsn(opcode, label);
		endPop(poppedSlots);
		action.run();
		methodVisitor().visitLabel(label);
	}

	public void ifFalse(Runnable action) { branchAround(action, IFNE, 1); }
	public void ifTrue(Runnable action) { branchAround(action, IFEQ, 1); }

	/**
	 * Finish building the class, load it with its own ClassLoader, and instantiate it.
	 * @return A new instance of the class.
	 */
	public T buildInstance() {
		generateConstructor(sourceFileOrigin);
		classVisitor.visitEnd();

		Constructor<?> ctor = new CustomClassLoader()
			.loadThemBytes(dottyName, classWriter.toByteArray())
			.getConstructors()[0];
		Object[] args = curriedFields.stream().map(CurriedField::value).toArray();
		try {
			return supertype.cast(ctor.newInstance(args));
		} catch (InstantiationException | IllegalAccessException | VerifyError | InvocationTargetException e) {
			throw new AssertionError("Should be able to instantiate the generated class", e);
		}
	}

	/**
	 * Bookkeeping before any instruction that causes a net increase of 1 in the operand stack depth.
	 */
	private void beginPush() {
		emitLineNumberInfo();
		currentMethod.pushSlots(1);
	}

	/**
	 * Bookkeeping after any instruction that causes a net reduction in the operand stack depth.
	 */
	private void endPop(int count) {
		currentMethod.popSlots(count);
	}

	private void emitLineNumberInfo() {
		StackTraceElement bestFrame = sourceFileOrigin;

		// Try to find a more specific line number. Due to the limits of
		// Java's source line number info, it needs to be in the same file
		// as sourceFileOrigin; try to pick the deepest frame in that file.
		//
		String sourceFileName = sourceFileOrigin.getFileName();
		for (StackTraceElement frame: new Exception().getStackTrace()) {
			if (Objects.equals(sourceFileName, frame.getFileName())) {
				bestFrame = frame;
				break;
			}
		}

		Label label = new Label();
		methodVisitor().visitLabel(label);
		methodVisitor().visitLineNumber(bestFrame.getLineNumber(), label);
	}

	private MethodVisitor methodVisitor() {
		if (currentMethod == null) {
			throw new IllegalStateException("No method in progress");
		} else {
			return currentMethod.methodVisitor;
		}
	}

	private final class CustomClassLoader extends ClassLoader {
		CustomClassLoader() {
			super(parentClassLoader);
		}

		public Class<?> loadThemBytes(String dottyName, byte[] b) {
			return defineClass(dottyName, b, 0, b.length);
		}
	}

	public static final Type OBJECT_TYPE = Type.getType(Object.class);
}
