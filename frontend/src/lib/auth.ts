export type AdminRole = "admin" | "content_manager";

export interface AdminUser {
  id?: string;
  email: string;
  name: string;
  role?: AdminRole;
  permissions?: string[];
}

/** True if `user` may reach a section that requires `perm`. Admins implicitly allow. */
export function hasPermission(user: AdminUser | null | undefined, perm: string): boolean {
  if (!user) return false;
  if (user.role === "admin") return true;
  return (user.permissions || []).includes(perm);
}

export function isAdmin(user: AdminUser | null | undefined): boolean {
  return !!user && user.role === "admin";
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
