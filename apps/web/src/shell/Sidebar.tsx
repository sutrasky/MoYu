import clsx from 'clsx';

import type { ConversationSummary, RepositorySummary } from '@moyu/contracts';

import { Icon } from '../components/Icon';
import {
  formatUpdatedAt,
  repositoryRuntimeLabel,
  taskStateLabel,
  runtimeResourceLabel,
  waitingForLabel,
} from '../lib/format';
import type { ThemeMode } from '../theme/ThemeProvider';
import styles from './WorkspaceShell.module.css';

type Mode = 'work' | 'chat';
type LoadStatus = 'idle' | 'loading' | 'ready' | 'error';

interface SidebarProps {
  mode: Mode;
  onModeChange: (mode: Mode) => void;
  repositories: RepositorySummary[];
  repositoriesStatus: LoadStatus;
  repositoriesError: string | null;
  selectedRepositoryId: string | null;
  onSelectRepository: (id: string) => void;
  onReloadRepositories: () => void;
  conversations: ConversationSummary[];
  conversationsStatus: LoadStatus;
  conversationsError: string | null;
  conversationsHasMore: boolean;
  conversationsMoreStatus: 'idle' | 'loading' | 'error';
  conversationsMoreError: string | null;
  onLoadMoreConversations: () => void;
  selectedConversationId: string | null;
  onSelectConversation: (id: string) => void;
  onReloadConversations: () => void;
  onNewTask: () => void;
  onOpenAddRepository: () => void;
  onRepositoryAction: (action: 'wake' | 'sleep' | 'retry' | 'retry_stop') => void;
  repositoryActionBusy: boolean;
  repositoryActionError: string | null;
  onOpenActivity: () => void;
  onOpenSettings: () => void;
  ownerName: string;
  themeMode: ThemeMode;
  onCycleTheme: () => void;
  mobileOpen: boolean;
  onClose: () => void;
  onLogout: () => void;
}

export function Sidebar({
  mode,
  onModeChange,
  repositories,
  repositoriesStatus,
  repositoriesError,
  selectedRepositoryId,
  onSelectRepository,
  onReloadRepositories,
  conversations,
  conversationsStatus,
  conversationsError,
  conversationsHasMore,
  conversationsMoreStatus,
  conversationsMoreError,
  onLoadMoreConversations,
  selectedConversationId,
  onSelectConversation,
  onReloadConversations,
  onNewTask,
  onOpenAddRepository,
  onRepositoryAction,
  repositoryActionBusy,
  repositoryActionError,
  onOpenActivity,
  onOpenSettings,
  ownerName,
  themeMode,
  onCycleTheme,
  mobileOpen,
  onClose,
  onLogout,
}: SidebarProps) {
  const selectedRepository =
    repositories.find((repository) => repository.id === selectedRepositoryId) ?? null;
  const runtime = selectedRepository?.runtime ?? null;
  const waiting = waitingForLabel(runtime?.waitingFor);
  const resource = runtimeResourceLabel(runtime?.resource);

  return (
    <aside
      className={clsx(styles.sidebar, mobileOpen && styles.sidebarOpen)}
      aria-label="工作台导航"
    >
      <div className={styles.brand}>
        <span className={styles.brandMark} aria-hidden="true">
          Mo
        </span>
        <div className={styles.brandText}>
          <strong>MoYu</strong>
          <span>个人智能体工作台</span>
        </div>
        <button
          type="button"
          className={styles.mobileClose}
          aria-label="关闭导航"
          onClick={onClose}
        >
          <Icon name="x" />
        </button>
      </div>

      <div className={styles.modeSwitch} role="tablist" aria-label="模式">
        <button
          type="button"
          role="tab"
          aria-selected={mode === 'work'}
          className={clsx(styles.modeButton, mode === 'work' && styles.modeButtonActive)}
          onClick={() => onModeChange('work')}
        >
          <Icon name="work" />
          Work
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={mode === 'chat'}
          className={clsx(styles.modeButton, mode === 'chat' && styles.modeButtonActive)}
          onClick={() => onModeChange('chat')}
        >
          <Icon name="chat" />
          Chat
        </button>
      </div>

      <section className={styles.sidebarSection} aria-labelledby="repository-heading">
        <div className={styles.sectionHeading}>
          <h2 id="repository-heading">仓库</h2>
          <button
            type="button"
            className={styles.iconButton}
            onClick={onReloadRepositories}
            aria-label="刷新仓库列表"
            title="刷新仓库列表"
          >
            <Icon name="branch" size={15} />
          </button>
        </div>

        {repositoriesStatus === 'loading' ? (
          <p className={styles.sectionHint}>正在读取仓库…</p>
        ) : repositoriesStatus === 'error' ? (
          <div className={styles.sectionError}>
            <p>{repositoriesError ?? '仓库列表不可用'}</p>
            <button type="button" className={styles.textButton} onClick={onReloadRepositories}>
              重试
            </button>
          </div>
        ) : repositories.length === 0 ? (
          <div className={styles.emptyCard}>
            <Icon name="cloud" />
            <p>尚未添加仓库</p>
            <span>网页新增仓库、实例调度与资源控制将在 M5 接入；当前使用已配置仓库。</span>
          </div>
        ) : (
          <label className={styles.repositorySelect}>
            <span className="visually-hidden">选择仓库</span>
            <select
              value={selectedRepositoryId ?? ''}
              onChange={(event) => onSelectRepository(event.target.value)}
            >
              {repositories.map((repository) => (
                <option key={repository.id} value={repository.id}>
                  {repository.fullName}
                </option>
              ))}
            </select>
            <Icon name="chevron-down" size={15} />
          </label>
        )}

        <button type="button" className={styles.addRepositoryButton} onClick={onOpenAddRepository}>
          <Icon name="plus" size={15} />
          添加仓库
        </button>

        {selectedRepository && runtime ? (
          <div className={styles.repositoryRuntime} aria-live="polite">
            <div className={styles.runtimeTitle}>
              <span
                className={clsx(styles.runtimeDot, styles[`runtime-${runtime.state}`])}
                aria-hidden="true"
              />
              <strong>{repositoryRuntimeLabel(runtime)}</strong>
              <span className={styles.runtimeListener}>
                {runtime.listener === 'active'
                  ? 'GitHub 监听中'
                  : runtime.listener === 'paused'
                    ? 'GitHub 监听暂停'
                    : '监听状态未知'}
              </span>
            </div>
            <p className={styles.runtimeReason}>{runtime.reason}</p>
            <div className={styles.runtimeMeta}>
              <span>
                执行名额：
                {runtime.executionSlot === 'busy'
                  ? '使用中'
                  : runtime.executionSlot === 'free'
                    ? '空闲'
                    : '未知'}
              </span>
              {waiting ? <span>{waiting}</span> : null}
              {resource ? <span>{resource}</span> : null}
              {runtime.lease ? <span>执行代次 {runtime.lease.fencingToken}</span> : null}
              {runtime.lastProgressAt ? (
                <span>最后进展 {formatUpdatedAt(runtime.lastProgressAt)}</span>
              ) : (
                <span>暂无有效进展</span>
              )}
            </div>
            <div className={styles.runtimeActions}>
              {runtime.actions.map((action) => (
                <button
                  key={action.action}
                  type="button"
                  className={styles.textButton}
                  disabled={!action.enabled || repositoryActionBusy}
                  title={action.unavailableReason ?? action.label}
                  onClick={() => onRepositoryAction(action.action)}
                >
                  {repositoryActionBusy ? '处理中…' : action.label}
                </button>
              ))}
            </div>
            {repositoryActionError ? (
              <p className={styles.runtimeError} role="alert">
                {repositoryActionError}
              </p>
            ) : null}
          </div>
        ) : null}
      </section>

      <section className={styles.sidebarSection} aria-labelledby="conversation-heading">
        <div className={styles.sectionHeading}>
          <h2 id="conversation-heading">会话</h2>
          <div className={styles.sectionHeadingActions}>
            <span className={styles.count}>{conversations.length}</span>
            <button
              type="button"
              className={styles.iconButton}
              onClick={onReloadConversations}
              aria-label="刷新会话列表"
              title="刷新会话列表"
              disabled={!selectedRepositoryId}
            >
              <Icon name="session" size={15} />
            </button>
          </div>
        </div>

        {!selectedRepositoryId ? (
          <p className={styles.sectionHint}>先选择仓库，再查看该仓库的会话。</p>
        ) : conversationsStatus === 'loading' && conversations.length === 0 ? (
          <p className={styles.sectionHint}>正在读取会话…</p>
        ) : conversationsStatus === 'error' ? (
          <div className={styles.sectionError}>
            <p>{conversationsError ?? '会话列表不可用'}</p>
            <button type="button" className={styles.textButton} onClick={onReloadConversations}>
              重试
            </button>
          </div>
        ) : conversations.length === 0 ? (
          <div className={styles.emptyList}>
            <Icon name="task" />
            <p>还没有任务会话</p>
            <span>新建任务后，会话会按仓库汇总在这里。</span>
          </div>
        ) : (
          <ul className={styles.conversationList}>
            {conversations.map((conversation) => {
              const state = conversation.task?.state ?? null;
              return (
                <li key={conversation.id}>
                  <button
                    type="button"
                    className={clsx(
                      styles.conversationItem,
                      conversation.id === selectedConversationId && styles.conversationItemActive,
                    )}
                    onClick={() => onSelectConversation(conversation.id)}
                    aria-current={conversation.id === selectedConversationId ? 'true' : undefined}
                    data-conversation-id={conversation.id}
                  >
                    <span className={styles.conversationItemTop}>
                      <span className={styles.conversationTitle}>
                        {conversation.title ?? '未命名任务'}
                      </span>
                      <span className={styles.conversationState} data-state={state ?? 'unknown'}>
                        {taskStateLabel(state)}
                      </span>
                    </span>
                    <span className={styles.conversationTime}>
                      {formatUpdatedAt(conversation.updatedAt)}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        )}

        {conversationsHasMore || conversationsMoreStatus === 'error' ? (
          <div className={styles.sectionMore}>
            {conversationsMoreStatus === 'error' ? (
              <>
                <p>{conversationsMoreError ?? '加载更多任务失败'}</p>
                <button
                  type="button"
                  className={styles.textButton}
                  onClick={onLoadMoreConversations}
                >
                  重试
                </button>
              </>
            ) : (
              <button
                type="button"
                className={styles.textButton}
                onClick={onLoadMoreConversations}
                disabled={conversationsMoreStatus === 'loading'}
              >
                {conversationsMoreStatus === 'loading' ? '正在加载更多任务…' : '加载更多任务'}
              </button>
            )}
          </div>
        ) : null}

        <button
          type="button"
          className={styles.newTaskButton}
          onClick={onNewTask}
          disabled={!selectedRepositoryId}
          title={selectedRepositoryId ? '在当前仓库新建任务' : '先选择仓库'}
        >
          <Icon name="plus" size={15} />
          新建任务
        </button>
      </section>

      <nav className={styles.sidebarNav} aria-label="工作台入口">
        <button type="button" className={styles.navButton} onClick={onOpenActivity}>
          <Icon name="bell" />
          活动中心
          <span className={styles.navMeta}>M6</span>
        </button>
        <button type="button" className={styles.navButton} onClick={onOpenSettings}>
          <Icon name="settings" />
          设置
        </button>
      </nav>

      <footer className={styles.sidebarFooter}>
        <div className={styles.ownerRow}>
          <span className={styles.avatar} aria-hidden="true">
            {ownerName.slice(0, 1).toUpperCase()}
          </span>
          <div>
            <strong>{ownerName}</strong>
            <span>单所有者</span>
          </div>
        </div>
        <div className={styles.footerActions}>
          <button
            type="button"
            className={styles.iconButton}
            onClick={onCycleTheme}
            title={`主题：${themeMode}`}
            aria-label={`切换主题，当前 ${themeMode}`}
          >
            <Icon
              name={themeMode === 'dark' ? 'moon' : themeMode === 'light' ? 'sun' : 'monitor'}
            />
          </button>
          <button
            type="button"
            className={styles.iconButton}
            onClick={onLogout}
            title="退出登录"
            aria-label="退出登录"
          >
            <Icon name="logout" />
          </button>
        </div>
      </footer>
    </aside>
  );
}
