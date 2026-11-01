import js from "@eslint/js";
import hooks from "eslint-plugin-react-hooks";
import ts from "typescript-eslint";

export default ts.config(js.configs.recommended, ...ts.configs.recommended, {
  plugins: { "react-hooks": hooks },
  rules: { ...hooks.configs.recommended.rules, "@typescript-eslint/no-unused-vars": ["error", { ignoreRestSiblings: true }] },
});
