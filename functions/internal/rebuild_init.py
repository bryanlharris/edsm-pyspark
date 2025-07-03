import os
import ast


def extract_function_names(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=filepath)
    return [node.name for node in tree.body if isinstance(node, ast.FunctionDef)]


def generate_init_file(directory):
    lines = []
    all_exports = []
    for filename in sorted(os.listdir(directory)):
        fullpath = os.path.join(directory, filename)
        if filename.endswith(".py") and filename != "__init__.py" and os.path.isfile(fullpath):
            modname = filename[:-3]
            func_names = extract_function_names(fullpath)
            if func_names:
                lines.append(f"from .{modname} import " + ", ".join(func_names))
                all_exports.extend(func_names)
    if all_exports:
        lines.append("")
        lines.append("__all__ = [" + ", ".join(f'\"{name}\"' for name in all_exports) + "]")
    init_path = os.path.join(directory, "__init__.py")
    with open(init_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def build_all_init(project_root):
    os.chdir(project_root)
    errs = []
    for layer in ["functions"]:
        try:
            generate_init_file(layer)
        except Exception as e:
            errs.append(f"Failed to generate __init__.py for {layer}: {e}")
    if errs:
        raise RuntimeError("Errors in build_all_init: " + "; ".join(errs))
    print("Sanity check: Python __init__.py files rebuild check passed.")
