import { useMutation } from '@tanstack/react-query'
import { useState, type SyntheticEvent } from 'react'
import { Link, Navigate, useLocation, useNavigate } from 'react-router'
import { login, register } from '../api/client'
import { Button, Card, Field, Message } from '../components/ui'
import { describeError } from '../lib/format'
import { useAuth } from '../stores/auth'

interface Props {
  mode: 'login' | 'register'
}

export function AuthPage({ mode }: Props) {
  const status = useAuth((s) => s.status)
  const navigate = useNavigate()
  const location = useLocation()
  const from = (location.state as { from?: string } | null)?.from ?? '/'

  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')

  const submit = useMutation({
    mutationFn: () => (mode === 'login' ? login({ email, password }) : register({ email, password })),
    onSuccess: () => void navigate(from, { replace: true }),
  })

  if (status === 'authenticated') return <Navigate to={from} replace />

  const onSubmit = (e: SyntheticEvent) => {
    e.preventDefault()
    submit.mutate()
  }

  const isLogin = mode === 'login'
  return (
    <div className="mx-auto max-w-sm">
      <Card>
        <h1 className="mb-4 text-xl font-bold">{isLogin ? 'Sign in' : 'Create your account'}</h1>
        <form onSubmit={onSubmit} className="space-y-4">
          <Field
            label="Email"
            type="email"
            autoComplete="email"
            required
            value={email}
            onChange={(e) => {
              setEmail(e.target.value)
            }}
          />
          <Field
            label="Password"
            type="password"
            autoComplete={isLogin ? 'current-password' : 'new-password'}
            required
            minLength={isLogin ? undefined : 8}
            value={password}
            onChange={(e) => {
              setPassword(e.target.value)
            }}
          />
          {!isLogin && <p className="text-xs text-slate-500">At least 8 characters.</p>}
          {submit.isError && <Message tone="error">{describeError(submit.error)}</Message>}
          <Button type="submit" className="w-full" disabled={submit.isPending}>
            {submit.isPending ? 'Please wait…' : isLogin ? 'Sign in' : 'Create account'}
          </Button>
        </form>
        <p className="mt-4 text-center text-sm text-slate-600">
          {isLogin ? (
            <>
              New here?{' '}
              <Link to="/register" state={{ from }} className="font-medium text-indigo-600 hover:underline">
                Create an account
              </Link>
            </>
          ) : (
            <>
              Already registered?{' '}
              <Link to="/login" state={{ from }} className="font-medium text-indigo-600 hover:underline">
                Sign in
              </Link>
            </>
          )}
        </p>
      </Card>
    </div>
  )
}
