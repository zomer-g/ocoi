export interface AdminUser {
  email: string;
  name: string;
}

export async function getMe(): Promise<AdminUser | null> {
  try {
    const res = await fetch("/api/v1/auth/me", { credentials: "include" });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

export function loginUrl(): string {
  return "/api/v1/auth/login";
}

export async function logout(): Promise<void> {
  await fetch("/api/v1/auth/logout", { method: "POST", credentials: "include" });
  window.location.href = "/admin/login";
}
