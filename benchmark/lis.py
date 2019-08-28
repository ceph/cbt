import re
import operator as op

# a mini s-expr interpreter
# inspired by https://norvig.com/lispy.html

Symbol = str
List   = list

class Lispy:
    @staticmethod
    def _tokenize(s):
        return s.replace('(',' ( ').replace(')',' ) ').split()

    @staticmethod
    def _atom(token):
        try:
            return int(token)
        except ValueError:
            try:
                return float(token)
            except ValueError:
                return Symbol(token)

    def _read_from_tokens(self, tokens):
        if len(tokens) == 0:
            raise SyntaxError('unexpected EOF while reading')
        token = tokens.pop(0)
        if token == '(':
            stmt = []
            while tokens[0] != ')':
                stmt.append(self._read_from_tokens(tokens))
            tokens.pop(0) # pop off ')'
            return stmt
        elif token == ')':
            raise SyntaxError('unexpected ")"')
        else:
            return self._atom(token)

    def parse(self, s):
        return self._read_from_tokens(self._tokenize(s))

    def eval(self, stmt, env):
        if isinstance(stmt, Symbol):
            return env.eval(stmt)
        elif isinstance(stmt, List):
            func = self.eval(stmt[0], env)
            args = [self.eval(exp, env) for exp in stmt[1:]]
            return func(*args)
        else:
            return stmt


class Env(dict):
    @staticmethod
    def near(lhs, rhs, abs_error):
        return (abs(lhs - rhs) / float(rhs)) <= abs_error

    def __init__(self, **locals):
        if locals:
            self.update(locals)
        # pass 'result' and 'baseline' to some functions
        self.update({
            'less': lambda: self.eval('result') < self.eval('baseline'),
            'greater': lambda: self.eval('result') > self.eval('baseline'),
            'near': lambda abs_error: self.near(self.eval('result'),
                                                self.eval('baseline'),
                                                abs_error),
            'or': op.or_})

    def find(self, var):
        if var in self:
            return self
        elif self.outer:
            return self.outer.find(var)
        else:
            raise NameError(var)

    def eval(self, var):
        return self.find(var)[var]
