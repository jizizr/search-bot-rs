use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct UserCache {
    /// lowercase username -> user_id
    name_to_id: Arc<DashMap<String, i64>>,
    /// user_id -> (display_name, username)
    id_to_info: Arc<DashMap<i64, UserInfo>>,
}

#[derive(Debug, Clone)]
struct UserInfo {
    display_name: String,
    username: Option<String>,
}

impl UserCache {
    pub fn new() -> Self {
        Self {
            name_to_id: Arc::new(DashMap::new()),
            id_to_info: Arc::new(DashMap::new()),
        }
    }

    /// Update cache with latest user info. Handles username changes by removing the old mapping.
    pub fn update(&self, user_id: i64, username: Option<&str>, display_name: String) {
        // Remove stale username mapping if the user changed their username
        if let Some(old_info) = self.id_to_info.get(&user_id) {
            if let Some(ref old_uname) = old_info.username {
                let old_lower = old_uname.to_lowercase();
                let new_lower = username.map(|u| u.to_lowercase());
                if Some(old_lower.as_str()) != new_lower.as_deref() {
                    self.name_to_id.remove(&old_lower);
                }
            }
        }

        if let Some(uname) = username {
            self.name_to_id.insert(uname.to_lowercase(), user_id);
        }

        self.id_to_info.insert(
            user_id,
            UserInfo {
                display_name,
                username: username.map(String::from),
            },
        );
    }

    /// Resolve @username to user_id. Returns None if username not seen.
    pub fn resolve_username(&self, username: &str) -> Option<i64> {
        let clean = username.trim_start_matches('@').to_lowercase();
        self.name_to_id.get(&clean).map(|v| *v)
    }

    /// Get display name for a user_id. Returns None if user not seen.
    pub fn get_display_name(&self, user_id: i64) -> Option<String> {
        self.id_to_info
            .get(&user_id)
            .map(|info| info.display_name.clone())
    }
}
